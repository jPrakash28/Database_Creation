import java.sql.*;
import java.io.*;

public class keywordsLoad {
	static final String URL = "jdbc:mysql://localhost:3306/?useSSL=false";//the url for localhost
	static final String USER = "root";//User for mysql
	static final String PASSWORD = "Ruby2004";//password for mysql
	
	public static void main(String[] args) {
		Connection connect = null;
		String csvFile;
		BufferedReader buffReader = null;
		String fileLine = "";
		String csvSplit = ",";
		
		//try statement that creates a table in Movies_Project
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			connect = DriverManager.getConnection(URL, USER, PASSWORD);
			
			Statement statement = connect.createStatement();
			
			//This creates the Movies_Project database if it does not exist
			String sql = "CREATE DATABASE IF NOT EXISTS Movies_Project ";
			statement.executeUpdate(sql);
			System.out.println("Movies_Project Database created");
			System.out.println();
			
			sql = "USE Movies_Project"; //Creates SQL String which stores "USE Movies_Project" command
			statement.execute(sql);//Executes the original command
			
			sql = "DROP TABLE IF EXISTS keywords";
            statement.execute(sql);
			
			csvFile = "/Users/josh/Documents/keywords.csv";//Stores the file location of keywords to csvFile
			
			System.out.println("Creating Keywords Table...");
			System.out.println();
			
			//The main sql code that creates a keywords table
			sql = "CREATE TABLE IF NOT EXISTS keywords ("
					+ "movie_id FLOAT NOT NULL,"
					+ "keyword_id FLOAT NOT NULL,"
					+ "keyword_name VARCHAR(60) NULL,"
					+ "FOREIGN KEY (movie_id) REFERENCES movies(movie_id),"
					+ "PRIMARY KEY (movie_id, keyword_id))";
			
			statement.execute(sql);//executes the above code
			
			buffReader = new BufferedReader(new FileReader(csvFile));//reads the file
			
			//while loop to keep adding information from the file to keywords table
			while((fileLine = buffReader.readLine()) != null) {
				
				int i = -1;
				String[] keywordToken = fileLine.split(csvSplit);//keywordToken array stores the split information
				
				sql = "INSERT INTO keywords VALUES (?, ?, ?)";//sql command to store the information from file to the table
				PreparedStatement prepStatement = connect.prepareStatement(sql);
				
				prepStatement.clearParameters();
				
				prepStatement.setFloat(1,  Float.parseFloat(keywordToken[++i]));//adds the movies id
				prepStatement.setFloat(2,  Float.parseFloat(keywordToken[++i]));//add the keywords id
				prepStatement.setString(3,  keywordToken[++i]);//adds the keyword name
				
				try {
					prepStatement.executeUpdate();//executes the statements above
				}
				//Catches any exceptions
				catch (Exception e) {
					
				}
			
				
				prepStatement.close();
				
			}//end while loop
			
			System.out.println("Keywords Table Created");
			System.out.println();
			
		}//end main try
		//catch statement which catches the SQL Exceptions
		catch (Exception e) {
			System.out.println(fileLine);
			System.out.println("SQL Exception");
			e.printStackTrace();
		}

	}

}
