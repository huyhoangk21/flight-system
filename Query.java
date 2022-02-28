import java.io.FileInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Runs queries against a back-end database
 */
public class Query
{
    private String configFilename;
    private Properties configProps = new Properties();

    private String jSQLDriver;
    private String jSQLUrl;
    private String jSQLUser;
    private String jSQLPassword;

    // DB Connection
    private Connection conn;

    // Logged In User
    private String username; // customer username is unique

    // Canned queries
    private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
    private PreparedStatement checkFlightCapacityStatement;

    private static final String CHECK_USER = "SELECT * FROM Users WHERE username = ?";
    private PreparedStatement checkUserStatement;

    private static final String INSERT_USER = "INSERT INTO Users VALUES (?, ?, ?)";
    private PreparedStatement insertUserStatement;

    private static final String DIRECT_SEARCH =
        "SELECT TOP (?) * " +
        "FROM Flights " +
        "WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? AND canceled = 0 " +
        "ORDER BY actual_time, fid";
    private PreparedStatement directSearchStatement;

    private static final String INDIRECT_SEARCH =
        "SELECT TOP (?) f1.fid fid1, f1.day_of_month day1, f1.carrier_id carrier1, f1.flight_num num1, f1.origin_city origin1, f1.dest_city dest1, f1.actual_time time1, f1.capacity capacity1, f1.price price1, " +
        "f2.fid fid2, f1.day_of_month day2, f2.carrier_id carrier2, f2.flight_num num2, f2.origin_city origin2, f2.dest_city dest2, f2.actual_time time2, f2.capacity capacity2, f2.price price2 " +
        "FROM Flights f1, Flights f2 " +
        "WHERE f1.origin_city = ? AND f1.dest_city = f2.origin_city AND f2.dest_city = ? AND f1.day_of_month = ? AND f1.day_of_month = f2.day_of_month AND f1.canceled = 0 AND f2.canceled = 0 "+
        "ORDER BY (f1.actual_time + f2.actual_time), f1.fid, f2.fid";
    private PreparedStatement indirectSearchStatement;

    private static final String CHECK_FLIGHT = "SELECT * " +
                                               "FROM Flights " +
                                               "WHERE fid = ?";
    private PreparedStatement checkFlightStatement;

    private static final String CHECK_RESERVATION_BY_USERNAME = "SELECT * " +
                                                                "FROM Reservations " +
                                                                "WHERE username = ? " +
                                                                "ORDER BY rid";
    private PreparedStatement checkReservationByUsernameStatement;

    private static final String COUNT_SEAT = "SELECT ISNULL(" +
                                             "(SELECT COUNT(*) " +
                                             "FROM ((SELECT fid1 FROM Reservations) UNION ALL (SELECT fid2 FROM Reservations)) AS N " +
                                             "WHERE N.fid1 = ? " +
                                             "GROUP BY N.fid1), 0) AS count";

    private PreparedStatement countSeatStatement;

    private static final String INSERT_RESERVATION = "INSERT INTO Reservations VALUES(0, ?, ?, ?)";
    private PreparedStatement insertReservationStatement;

    private static final String UPDATE_USER_BALANCE = "UPDATE Users SET balance = ? WHERE username = ?";
    private PreparedStatement updateUserBalanceStatement;

    private static final String UPDATE_RESERVATION_PAID = "UPDATE Reservations SET paid = 1 WHERE rid = ?";
    private PreparedStatement updateReservationPaidStatement;

    private static final String CHECK_RESERVATION_BY_USERNAME_AND_RID = "SELECT * " +
                                                                        "FROM Reservations " +
                                                                        "WHERE username = ? AND rid =? ";
    private PreparedStatement checkReservationByUsernameAndRidStatement;

    private static final String DELETE_RESERVATION = "DELETE FROM Reservations WHERE rid = ?";
    private PreparedStatement deleteReservationStatement;

    // transactions
    private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
    private PreparedStatement beginTransactionStatement;

    private static final String COMMIT_SQL = "COMMIT TRANSACTION";
    private PreparedStatement commitTransactionStatement;

    private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
    private PreparedStatement rollbackTransactionStatement;


    //---------
    private boolean loggedIn = false;
    private List<Itinerary> itineraries = new ArrayList<>();
    private static final int CANCEL = 0;
    private static final int PAY = 1;

    public class Flight
    {
        private int fid;
        private int dayOfMonth;
        private String carrierId;
        private String flightNum;
        private String originCity;
        private String destCity;
        private int time;
        private int capacity;
        private int price;

        public Flight(int fid, int dayOfMonth, String carrierId, String flightNum, String originCity, String destCity, int time, int capacity, int price) {
            this.fid = fid;
            this.dayOfMonth = dayOfMonth;
            this.carrierId = carrierId;
            this.flightNum = flightNum;
            this.originCity = originCity;
            this.destCity = destCity;
            this.time = time;
            this.capacity = capacity;
            this.price = price;
        }

        @Override
        public String toString()
        {
            return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
                   " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
                   " Capacity: " + capacity + " Price: " + price + "\n";
        }

        public int getFid() {
            return this.fid;
        }

        public int getTime() {
            return this.time;
        }

        public int getPrice() {
            return this.price;
        }

        public int getDay() {
            return dayOfMonth;
        }
    }

    public class Itinerary implements Comparable<Itinerary> {
        private Flight f1;
        private Flight f2;
        private int size;

        private Itinerary(Flight f1, Flight f2, int size) {
            this.f1 = f1;
            this.f2 = f2;
            this.size = size;
        }

        public Itinerary(Flight f1, Flight f2) {
            this(f1, f2, 2);
        }

        public Itinerary(Flight f1) {
            this(f1, null, 1);
        }

        public Flight first() {
            return this.f1;
        }

        public Flight second() {
            return this.f2;
        }

        public int size() {
            return size;
        }

        public int compareTo(Itinerary other) {
            int thisTime = this.f1.getTime() + (this.f2==null ? 0 : this.f2.getTime());
            int otherTime = other.first().getTime() + (other.second()==null ? 0 : other.second().getTime());
            return thisTime - otherTime;
        }

        public String toString() {
            return this.f1.toString() + (this.f2==null ? "" : this.f2.toString());
        }


    }

    public Query(String configFilename)
    {
        this.configFilename = configFilename;
    }

    /* Connection code to SQL Azure.  */
    public void openConnection() throws Exception
    {
        configProps.load(new FileInputStream(configFilename));

        jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
        jSQLUrl = configProps.getProperty("flightservice.url");
        jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
        jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

        /* load jdbc drivers */
        Class.forName(jSQLDriver).newInstance();

        /* open connections to the flights database */
        conn = DriverManager.getConnection(jSQLUrl, // database
                                           jSQLUser, // user
                                           jSQLPassword); // password

        conn.setAutoCommit(true); //by default automatically commit after each statement

        /* You will also want to appropriately set the transaction's isolation level through:
           conn.setTransactionIsolation(...)
           See Connection class' JavaDoc for details.
         */
    }

    public void closeConnection() throws Exception
    {
        conn.close();
    }

    /**
     * Clear the data in any custom tables created. Do not drop any tables and do not
     * clear the flights table. You should clear any tables you use to store reservations
     * and reset the next reservation ID to be 1.
     */
    public void clearTables ()
    {
        String deleteReservations = "DELETE FROM Reservations";
        String deleteUsers = "DELETE FROM Users";
        String resetReservationID = "DBCC CHECKIDENT ('Reservations', RESEED, 0)";
        try {
            PreparedStatement s1 = conn.prepareStatement(deleteReservations);
            s1.executeUpdate();
            PreparedStatement s2 = conn.prepareStatement(deleteUsers);
            s2.executeUpdate();
            PreparedStatement s3 = conn.prepareStatement(resetReservationID);
            s3.executeUpdate();
        } catch (SQLException e) {}
    }

    /**
     * prepare all the SQL statements in this method.
     * "preparing" a statement is almost like compiling it.
     * Note that the parameters (with ?) are still not filled in
     */
    public void prepareStatements() throws Exception
    {
        beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
        commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
        rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);

        checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);

        /* add here more prepare statements for all the other queries you need */
        /* . . . . . . */
        checkUserStatement = conn.prepareStatement(CHECK_USER);
        insertUserStatement = conn.prepareStatement(INSERT_USER);
        directSearchStatement = conn.prepareStatement(DIRECT_SEARCH);
        indirectSearchStatement = conn.prepareStatement(INDIRECT_SEARCH);
        checkFlightStatement = conn.prepareStatement(CHECK_FLIGHT);
        checkReservationByUsernameStatement = conn.prepareStatement(CHECK_RESERVATION_BY_USERNAME);
        countSeatStatement = conn.prepareStatement(COUNT_SEAT);
        insertReservationStatement = conn.prepareStatement(INSERT_RESERVATION, Statement.RETURN_GENERATED_KEYS);
        updateUserBalanceStatement = conn.prepareStatement(UPDATE_USER_BALANCE);
        updateReservationPaidStatement = conn.prepareStatement(UPDATE_RESERVATION_PAID);
        checkReservationByUsernameAndRidStatement =  conn.prepareStatement(CHECK_RESERVATION_BY_USERNAME_AND_RID);
        deleteReservationStatement = conn.prepareStatement(DELETE_RESERVATION);
    }

    /**
     * Takes a user's username and password and attempts to log the user in.
     *
     * @param username
     * @param password
     *
     * @return If someone has already logged in, then return "User already logged in\n"
     * For all other errors, return "Login failed\n".
     *
     * Otherwise, return "Logged in as [username]\n".
     */
    public String transaction_login(String username, String password)
    {
        if (loggedIn) {
            return "User already logged in\n";
        }
        String errorMessage = "Login failed\n";
        try {
            beginTransaction();
            String passwordSQL = getUserPassword(username);
            commitTransaction();
            if (passwordSQL.equals(password)) {
                this.username = username;
                loggedIn = true;
                itineraries.clear();
                return "Logged in as " + this.username + "\n";
            }
        } catch (SQLException e) {
            try {
                rollbackTransaction();
            } catch (SQLException e1) {}
            return errorMessage;
        }
        return errorMessage;
    }

    /**
     * Implement the create user function.
     *
     * @param username new user's username. User names are unique the system.
     * @param password new user's password.
     * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
     *
     * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
     */
    public String transaction_createCustomer (String username, String password, int initAmount)
    {
        String errorMessage = "Failed to create user\n";
        if (initAmount < 0) {
            return errorMessage;
        }
        try {
            beginTransaction();
            String passwordSQL = getUserPassword(username);
            commitTransaction();
            if(passwordSQL.equals("")) {
                beginTransaction();
                insertUser(username, password, initAmount);
                commitTransaction();
                return "Created user " + username + "\n";
            }
        } catch (SQLException e) {
            try {
                rollbackTransaction();
            } catch (SQLException e1) {}
            return errorMessage;
        }
        return errorMessage;
    }

    /**
     * Implement the search function.
     *
     * Searches for flights from the given origin city to the given destination
     * city, on the given day of the month. If {@code directFlight} is true, it only
     * searches for direct flights, otherwise is searches for direct flights
     * and flights with two "hops." Only searches for up to the number of
     * itineraries given by {@code numberOfItineraries}.
     *
     * The results are sorted based on total flight time.
     *
     * @param originCity
     * @param destinationCity
     * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
     * @param dayOfMonth
     * @param numberOfItineraries number of itineraries to return
     *
     * @return If no itineraries were found, return "No flights match your selection\n".
     * If an error occurs, then return "Failed to search\n".
     *
     * Otherwise, the sorted itineraries printed in the following format:
     *
     * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
     * [first flight in itinerary]\n
     * ...
     * [last flight in itinerary]\n
     *
     * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
     * in each search should always start from 0 and increase by 1.
     *
     * @see Flight#toString()
     */
    public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
                                     int numberOfItineraries)
    {
        if (dayOfMonth > 0 && dayOfMonth < 31 && numberOfItineraries > 0) {
            try {
                itineraries.clear();
                beginTransaction();
                itineraries = directItineraries(originCity, destinationCity, dayOfMonth, numberOfItineraries);
                commitTransaction();
                int k = numberOfItineraries - itineraries.size();
                if (!directFlight && k > 0) {
                    beginTransaction();
                    itineraries.addAll(indirectItineraries(originCity, destinationCity, dayOfMonth, k));
                    commitTransaction();
                }
                if (itineraries.size()==0) {
                    return "No flights match your selection\n";
                } else {
                    String s = "";
                    Collections.sort(itineraries);
                    for (int i = 0; i<itineraries.size(); i++) {
                        s += "Itinerary " + i + ": " + itineraries.get(i).size() + " flight(s), " + (itineraries.get(i).first().getTime() + (itineraries.get(i).second()==null ? 0 : itineraries.get(i).second().getTime())) + " minutes\n";
                        s += itineraries.get(i).toString();
                    }
                    return s;
                }
            } catch (SQLException e) {
                try {
                    rollbackTransaction();
                } catch (SQLException e1) {}
                return "Failed to search\n";
            }
        } else {
            return "Failed to search\n";
        }
    }

    /**
     * Same as {@code transaction_search} except that it only performs single hop search and
     * do it in an unsafe manner.
     *
     * @param originCity
     * @param destinationCity
     * @param directFlight
     * @param dayOfMonth
     * @param numberOfItineraries
     *
     * @return The search results. Note that this implementation *does not conform* to the format required by
     * {@code transaction_search}.
     */
    private String transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight,
                                             int dayOfMonth, int numberOfItineraries)
    {
        StringBuffer sb = new StringBuffer();

        try
        {
            // one hop itineraries
            String unsafeSearchSQL =
                "SELECT TOP (" + numberOfItineraries + ") day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price "
                + "FROM Flights "
                + "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity + "\' AND day_of_month =  " + dayOfMonth + " "
                + "ORDER BY actual_time ASC";

            Statement searchStatement = conn.createStatement();
            ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);

            while (oneHopResults.next())
            {
                int result_dayOfMonth = oneHopResults.getInt("day_of_month");
                String result_carrierId = oneHopResults.getString("carrier_id");
                String result_flightNum = oneHopResults.getString("flight_num");
                String result_originCity = oneHopResults.getString("origin_city");
                String result_destCity = oneHopResults.getString("dest_city");
                int result_time = oneHopResults.getInt("actual_time");
                int result_capacity = oneHopResults.getInt("capacity");
                int result_price = oneHopResults.getInt("price");

                sb.append("Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: " + result_flightNum + " Origin: " + result_originCity + " Destination: " + result_destCity + " Duration: " + result_time + " Capacity: " + result_capacity + " Price: " + result_price + "\n");
            }
            oneHopResults.close();
        } catch (SQLException e) { e.printStackTrace(); }

        return sb.toString();
    }

    /**
     * Implements the book itinerary function.
     *
     * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
     *
     * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
     * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
     * If the user already has a reservation on the same day as the one that they are trying to book now, then return
     * "You cannot book two flights in the same day\n".
     * For all other errors, return "Booking failed\n".
     *
     * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
     * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
     * successful reservation is made by any user in the system.
     */
    public String transaction_book(int itineraryId)
    {
        if (!loggedIn) {
            return "Cannot book reservations, not logged in\n";
        }
        if (itineraries.size() > itineraryId && itineraryId >= 0) {
            String errorMessage = "Booking failed\n";
            try {
                Itinerary i = itineraries.get(itineraryId);
                beginTransaction();
                boolean sameDay = checkSameDayReservation(i.first().getDay());
                commitTransaction();
                if(sameDay) {
                    return "You cannot book two flights in the same day\n";
                } else {
                    beginTransaction();
                    int seat1 = getAvailableSeat(i.first());
                    int seat2 = getAvailableSeat(i.second());
                    if (seat1 > 0 && seat2 > 0) {
                        int id = updateReservations(this.username, i.first(), i.second());
                        commitTransaction();
                        return "Booked flight(s), reservation ID: " + id + "\n";
                    } else {
                        commitTransaction();
                        return errorMessage;
                    }
                }
            } catch (SQLException e) {
                try {
                    rollbackTransaction();
                } catch (SQLException e1) {}
                return errorMessage;
            }
        } else {
            return "No such itinerary " + itineraryId + "\n";
        }
    }

    /**
     * Implements the reservations function.
     *
     * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
     * If the user has no reservations, then return "No reservations found\n"
     * For all other errors, return "Failed to retrieve reservations\n"
     *
     * Otherwise return the reservations in the following format:
     *
     * Reservation [reservation ID] paid: [true or false]:\n"
     * [flight 1 under the reservation]
     * [flight 2 under the reservation]
     * Reservation [reservation ID] paid: [true or false]:\n"
     * [flight 1 under the reservation]
     * [flight 2 under the reservation]
     * ...
     *
     * Each flight should be printed using the same format as in the {@code Flight} class.
     *
     * @see Flight#toString()
     */
    public String transaction_reservations()
    {
        if(!loggedIn) {
            return "Cannot view reservations, not logged in\n";
        }
        String message = "Failed to retrieve reservations\n";
        try {
            beginTransaction();
            message = getReservation(this.username);
            commitTransaction();
        } catch (SQLException e) {
            try {
                rollbackTransaction();
            } catch (SQLException e1) {}
            return message;
        }
        return message;
    }

    /**
     * Implements the cancel operation.
     *
     * @param reservationId the reservation ID to cancel
     *
     * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
     * For all other errors, return "Failed to cancel reservation [reservationId]"
     *
     * If successful, return "Canceled reservation [reservationId]"
     *
     * Even though a reservation has been canceled, its ID should not be reused by the system.
     */
    public String transaction_cancel(int reservationId)
    {
        // only implement this if you are interested in earning extra credit for the HW!
        if (!loggedIn) {
            return "Cannot cancel reservations, not logged in\n";
        }
        String errorMessage = "Failed to cancel reservation " + reservationId + "\n";
        try {
            beginTransaction();
            int refund = getPriceSQL(this.username, reservationId, CANCEL);
            commitTransaction();
            if (refund == -1) {
                return errorMessage;
            } else {
                beginTransaction();
                int oldBalance = getBalance(this.username);
                updateBalance(this.username, refund+oldBalance);
                deleteReservation(reservationId);
                commitTransaction();
                return "Canceled reservation " + reservationId + "\n";
            }
        } catch (SQLException e) {
            try {
                rollbackTransaction();
            }catch (SQLException e1) {}
            e.printStackTrace();
            return errorMessage;
        }
    }

    /**
     * Implements the pay function.
     *
     * @param reservationId the reservation to pay for.
     *
     * @return If no user has logged in, then return "Cannot pay, not logged in\n"
     * If the reservation is not found / not under the logged in user's name, then return
     * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
     * If the user does not have enough money in their account, then return
     * "User has only [balance] in account but itinerary costs [cost]\n"
     * For all other errors, return "Failed to pay for reservation [reservationId]\n"
     *
     * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
     * where [balance] is the remaining balance in the user's account.
     */
    public String transaction_pay (int reservationId)
    {
        if(!loggedIn) {
            return "Cannot pay, not logged in\n";
        }
        String errorMessage = "Failed to pay for reservation " + reservationId + "\n";
        try {
            beginTransaction();
            int price = getPriceSQL(this.username, reservationId, PAY);
            int balance = getBalance(this.username);
            commitTransaction();
            int newBalance = balance - price;
            if (price <= 0) {
                return "Cannot find unpaid reservation " + reservationId + " under user: " + this.username + "\n";
            } else {
                if (newBalance > 0) {
                    beginTransaction();
                    updateBalance(this.username, newBalance);
                    updatePaid(reservationId);
                    commitTransaction();
                    return "Paid reservation: " + reservationId + " remaining balance: "+ newBalance + "\n";
                } else {
                    return "User has only " + balance + " in account but itinerary costs " + price + "\n";
                }
            }
        } catch (SQLException e) {
            try {
                rollbackTransaction();
            } catch (SQLException e1) {}
            e.printStackTrace();
            return errorMessage;
        }
    }

    /* some utility functions below */

    public void beginTransaction() throws SQLException
    {
        conn.setAutoCommit(false);
        beginTransactionStatement.executeUpdate();
    }

    public void commitTransaction() throws SQLException
    {
        commitTransactionStatement.executeUpdate();
        conn.setAutoCommit(true);
    }

    public void rollbackTransaction() throws SQLException
    {
        rollbackTransactionStatement.executeUpdate();
        conn.setAutoCommit(true);
    }

    /**
     * Shows an example of using PreparedStatements after setting arguments. You don't need to
     * use this method if you don't want to.
     */
    private int checkFlightCapacity(int fid) throws SQLException
    {
        checkFlightCapacityStatement.clearParameters();
        checkFlightCapacityStatement.setInt(1, fid);
        ResultSet results = checkFlightCapacityStatement.executeQuery();
        results.next();
        int capacity = results.getInt("capacity");
        results.close();

        return capacity;
    }

    //------------------------Methods--------------------------------
    private String getUserPassword(String username) throws SQLException {
        checkUserStatement.clearParameters();
        checkUserStatement.setString(1, username);
        ResultSet rs = checkUserStatement.executeQuery();
        String password = (rs.next() ? rs.getString("password") : "");
        rs.close();
        return password;
    }

    private void insertUser(String username, String password, int balance) throws SQLException {
        insertUserStatement.clearParameters();
        insertUserStatement.setString(1, username);
        insertUserStatement.setString(2, password);
        insertUserStatement.setInt(3, balance);
        insertUserStatement.executeUpdate();
    }

    private List<Itinerary> directItineraries(String origin, String dest, int day, int n) throws SQLException {
        List<Itinerary> direct = new ArrayList<Itinerary>();
        directSearchStatement.clearParameters();
        directSearchStatement.setInt(1, n);
        directSearchStatement.setString(2, origin);
        directSearchStatement.setString(3, dest);
        directSearchStatement.setInt(4, day);
        ResultSet rs = directSearchStatement.executeQuery();
        while (rs.next()) {
            Flight f1 = new Flight(rs.getInt("fid"), rs.getInt("day_of_month"), rs.getString("carrier_id"), rs.getString("flight_num"), rs.getString("origin_city"), rs.getString("dest_city"), rs.getInt("actual_time"), rs.getInt("capacity"), rs.getInt("price"));
            direct.add(new Itinerary(f1));
        }
        rs.close();
        return direct;
    }

    private List<Itinerary> indirectItineraries(String origin, String dest, int day, int n) throws SQLException {
        List<Itinerary> indirect = new ArrayList<Itinerary>();
        indirectSearchStatement.clearParameters();
        indirectSearchStatement.setInt(1, n);
        indirectSearchStatement.setString(2, origin);
        indirectSearchStatement.setString(3, dest);
        indirectSearchStatement.setInt(4, day);
        ResultSet rs = indirectSearchStatement.executeQuery();
        while (rs.next()) {
            Flight f1 = new Flight(rs.getInt("fid1"), rs.getInt("day1"), rs.getString("carrier1"), rs.getString("num1"), rs.getString("origin1"), rs.getString("dest1"), rs.getInt("time1"), rs.getInt("capacity1"), rs.getInt("price1"));
            Flight f2 = new Flight(rs.getInt("fid2"), rs.getInt("day2"), rs.getString("carrier2"), rs.getString("num2"), rs.getString("origin2"), rs.getString("dest2"), rs.getInt("time2"), rs.getInt("capacity2"), rs.getInt("price2"));
            indirect.add(new Itinerary(f1, f2));
        }
        rs.close();
        return indirect;
    }

    private int getDay(int fid) throws SQLException {
        checkFlightStatement.clearParameters();
        checkFlightStatement.setInt(1, fid);
        ResultSet rs = checkFlightStatement.executeQuery();
        rs.next();
        int day = rs.getInt("day_of_month");
        rs.close();
        return day;
    }

    private boolean checkSameDayReservation(int dayWantToBook) throws SQLException {
        checkReservationByUsernameStatement.clearParameters();
        checkReservationByUsernameStatement.setString(1, this.username);
        ResultSet rs = checkReservationByUsernameStatement.executeQuery();
        while (rs.next()) {
            int daySQL = getDay(rs.getInt("fid1"));
            if (daySQL == dayWantToBook) {
                rs.close();
                return true;
            }
        }
        rs.close();
        return false;
    }

    private int getAvailableSeat(Flight f) throws SQLException {
        if (f != null) {
            countSeatStatement.clearParameters();
            countSeatStatement.setInt(1, f.getFid());
            ResultSet rs = countSeatStatement.executeQuery();
            rs.next();
            int occupied = rs.getInt("count");
            rs.close();
            return checkFlightCapacity(f.getFid()) - occupied;
        } else {
            // if flight 2 of itinerary is null
            // return 1 -> always has at least an available seat
            return 1;
        }
    }

    private int updateReservations(String username, Flight f1, Flight f2) throws SQLException {
        insertReservationStatement.clearParameters();
        insertReservationStatement.setString(1, this.username);
        insertReservationStatement.setInt(2, f1.getFid());
        if (f2 != null) {
            insertReservationStatement.setInt(3, f2.getFid());
        } else {
            insertReservationStatement.setNull(3, Types.INTEGER);
        }
        insertReservationStatement.executeUpdate();
        ResultSet rs = insertReservationStatement.getGeneratedKeys();
        rs.next();
        int id = rs.getInt(1);
        rs.close();
        return id;
    }

    private String getReservation(String username) throws SQLException {
        checkReservationByUsernameStatement.clearParameters();
        checkReservationByUsernameStatement.setString(1, this.username);
        ResultSet rs = checkReservationByUsernameStatement.executeQuery();
        if (!rs.isBeforeFirst()) {
            return "No reservations found\n";
        } else {
            String message = "";
            while (rs.next()) {
                message+="Reservation " + rs.getInt("rid") + " paid: " + (rs.getInt("paid")==0 ? "false:\n" : "true:\n");
                message+= getFlightInfo(rs.getInt("fid1"));
                message+= getFlightInfo(rs.getInt("fid2"));
            }
            rs.close();
            return message;
        }
    }

    private String getFlightInfo(int fid) throws SQLException {
        String info = "";
        checkFlightStatement.clearParameters();
        checkFlightStatement.setInt(1, fid);
        ResultSet rs = checkFlightStatement.executeQuery();
        if (rs.next()) {
            Flight f1 = new Flight(rs.getInt("fid"), rs.getInt("day_of_month"), rs.getString("carrier_id"), rs.getString("flight_num"), rs.getString("origin_city"), rs.getString("dest_city"), rs.getInt("actual_time"), rs.getInt("capacity"), rs.getInt("price"));
            info = f1.toString();
        }
        rs.close();
        return info;
    }

    private int getPrice(int fid) throws SQLException {
        checkFlightStatement.clearParameters();
        checkFlightStatement.setInt(1, fid);
        ResultSet rs = checkFlightStatement.executeQuery();
        int price = 0;
        if (rs.next()) {
            price = rs.getInt("price");
        }
        rs.close();
        return price;
    }

    private int getBalance(String username) throws SQLException {
        checkUserStatement.clearParameters();
        checkUserStatement.setString(1, username);
        ResultSet rs = checkUserStatement.executeQuery();
        rs.next();
        int balance = rs.getInt("balance");
        rs.close();
        return balance;
    }

    private void updateBalance(String username, int newBalance) throws SQLException {
        updateUserBalanceStatement.clearParameters();
        updateUserBalanceStatement.setInt(1, newBalance);
        updateUserBalanceStatement.setString(2, username);
        updateUserBalanceStatement.executeUpdate();
    }

    private void updatePaid(int rid) throws SQLException {
        updateReservationPaidStatement.clearParameters();
        updateReservationPaidStatement.setInt(1, rid);
        updateReservationPaidStatement.executeUpdate();
    }

    private int getPriceSQL(String username, int rid, int f) throws SQLException {
        int price = -1;
        checkReservationByUsernameAndRidStatement.clearParameters();
        checkReservationByUsernameAndRidStatement.setString(1, username);
        checkReservationByUsernameAndRidStatement.setInt(2, rid);
        ResultSet rs = checkReservationByUsernameAndRidStatement.executeQuery();
        if (rs.isBeforeFirst()) {
            rs.next();
            if (f == CANCEL) {
                if (rs.getInt("paid") == 1) {
                    price = getPrice(rs.getInt("fid1")) + getPrice(rs.getInt("fid2"));
                } else {
                    price = 0;
                }
            } else {
                if (rs.getInt("paid") == 1) {
                    price = 0;
                } else {
                    price = getPrice(rs.getInt("fid1")) + getPrice(rs.getInt("fid2"));
                }
            }
        }
        rs.close();
        return price;
    }

    private void deleteReservation(int rid) throws SQLException {
        deleteReservationStatement.clearParameters();
        deleteReservationStatement.setInt(1, rid);
        deleteReservationStatement.executeUpdate();
    }
}
