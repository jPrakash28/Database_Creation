import java.sql.*;
import java.io.*;


public class movieLoad {
	static final String URL = "jdbc:mysql://localhost:3306/?useSSL=false";//local host url
	static final String USER = "root";//mysql username
	static final String PASSWORD = "Ruby2004";//mysql password

	public static void main(String[] args) {
		Connection connect = null;
		String csvFile;
		BufferedReader buffReader = null;
		String fileLine = "";
		String csvSplit = ",";
		
		try {
			
			Class.forName("com.mysql.cj.jdbc.Driver");
			connect = DriverManager.getConnection(URL, USER, PASSWORD);//connects to mysql
			
			Statement statement = connect.createStatement();
			
			//Creating Database
//			String sql = "DROP DATABASE Movies_Project";
//			statement.executeUpdate(sql);
			
			//This creates the Movies_Project database if it does not exist
			String sql = "CREATE DATABASE IF NOT EXISTS Movies_Project ";
			statement.executeUpdate(sql);
			System.out.println("Movies_Project Database created");
			System.out.println();
			
			sql = "USE Movies_PROJECT";//statement to use the Movies_Project database
			statement.executeUpdate(sql);
			
			csvFile = "/Users/josh/Documents/movie.csv";//file name/path
			
			System.out.println("Creating Movies Table...");
			System.out.println();
			
			//sql commands to create the movies table
			sql = "CREATE TABLE IF NOT EXISTS movies (" 
					+ "movie_id FLOAT NOT NULL,"
					+ "movie_title VARCHAR(100) NULL,"
					+ "movie_release_data VARCHAR(20) NULL," 
					+ "movie_language VARCHAR(10) NULL," 
					+ "movie_popularity FLOAT NULL,"
					+ "movie_revenue FLOAT NULL,"
					+ "movie_budget FLOAT NULL,"
					+ "movie_status VARCHAR(100) NULL,"
					+ "movie_tagline VARCHAR(1000) NULL,"
					+ "movie_vote_average FLOAT NULL,"
					+ "movie_vote_count FLOAT NULL,"
					+ "movie_runtime FLOAT NULL," 
					+ "PRIMARY KEY (movie_id),"
					+ "UNIQUE INDEX movie_id_unique (movie_id ASC) VISIBLE)";
			
			statement.execute(sql);
			
			buffReader = new BufferedReader(new FileReader(csvFile));//reads the file
			
			//while loop to keep adding information from the file to movies table
			while((fileLine = buffReader.readLine()) != null) {
				int i = -1;
				
				String[] movieToken = fileLine.split(csvSplit);//stores the split file line by line into movieToken array
				
				//command that inserts the information from file into movies table
				sql = "INSERT INTO movies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
				PreparedStatement prepStatement = connect.prepareStatement(sql);
				
				prepStatement.clearParameters();
				
				prepStatement.setFloat(1,  Float.parseFloat(movieToken[++i]));//adds the movie id
				prepStatement.setString(2, movieToken[++i]);//adds the movie name
				prepStatement.setString(3, movieToken[++i]);//adds the movie release date
				prepStatement.setString(4, movieToken[++i]);//adds the movie language
				prepStatement.setFloat(5, Float.parseFloat(movieToken[++i]));//adds the movie popularity
				prepStatement.setFloat(6, Float.parseFloat(movieToken[++i]));//adds the movie revenue
				prepStatement.setFloat(7, Float.parseFloat(movieToken[++i]));//adds the movie budget
				prepStatement.setString(8, movieToken[++i]);//adds the movie status
				prepStatement.setString(9, movieToken[++i]);//adds the movie tagline
				prepStatement.setFloat(10, Float.parseFloat(movieToken[++i]));//adds the movie vote average
				prepStatement.setFloat(11, Float.parseFloat(movieToken[++i]));//adds the movie vote count
				prepStatement.setFloat(12, Float.parseFloat(movieToken[++i]));//adds the movie runtime
				
				try {
					prepStatement.executeUpdate();//executes the above code
				}
				catch (Exception e) {}
				
				prepStatement.close();
				
			}// end of while loop
			
			System.out.println("Movies Table Created");
			System.out.println();
			
		}// end of try
		//catches the SQL exception
		catch (Exception e) {
			System.out.println(fileLine);
			System.out.println("SQL Exception");
			e.printStackTrace();
		}
	}

}
