import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class productionLoad {
	static final String URL = "jdbc:mysql://localhost:3306/?useSSL=false";//the url for localhost
	static final String USER = "root";//User for mysql
	static final String PASSWORD = "Ruby2004";//password for mysql
	
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
			
			//This creates the Movies_Project database if it does not exist
			String sql = "CREATE DATABASE IF NOT EXISTS Movies_Project ";
			statement.executeUpdate(sql);
			System.out.println("Movies_Project Database created");
			System.out.println();
			
			sql = "USE Movies_Project";//Creates SQL String which stores "USE Movies_Project" command
			statement.execute(sql);//Executes the original command
			
			sql = "DROP TABLE IF EXISTS production";
            statement.execute(sql);
			
			csvFile = "/Users/josh/Documents/production.csv";//Stores the file location of production.csv to csvFile
			
			System.out.println("Creating Production Table...");
			System.out.println();
			
			//The main sql code that creates a productions table
			sql = "CREATE TABLE IF NOT EXISTS production ("
					+ "movie_id FLOAT NOT NULL,"
					+ "production_id VARCHAR(50) NOT NULL,"
					+ "production_name VARCHAR(100),"
					+ "FOREIGN KEY (movie_id) REFERENCES movies(movie_id),"
					+ "PRIMARY KEY (movie_id, production_id))";
			
			statement.execute(sql);//executes the above code
			
			buffReader = new BufferedReader(new FileReader(csvFile));//reads the file
			
			//while loop to keep adding information from the file to productions table
			while((fileLine = buffReader.readLine()) != null) {
				
				int i = -1;
				String[] productionToken = fileLine.split(csvSplit);
				
				sql = "INSERT INTO production VALUES (?, ?, ?)";//sql command to store the information from file to the table
				PreparedStatement prepStatement = connect.prepareStatement(sql);
				
				prepStatement.clearParameters();
				
				prepStatement.setFloat(1, Float.parseFloat(productionToken[++i]));//adds movie id 
				prepStatement.setString(2, productionToken[++i]);//adds production id
				prepStatement.setString(3,  productionToken[++i]);//adds production name
				
				try {
					prepStatement.executeUpdate();//executes the above code 
				}//end of try
				catch(Exception e) {
					
				}//end of inner catch
				
				prepStatement.close();
				
			}//end of while
			
			System.out.println("Production Table Created");
			System.out.println();
			
		}//end of main try
		//catch statement which catches the SQL Exceptions
		catch(Exception e) {
			System.out.println(fileLine);
			System.out.println("SQL Exception");
			e.printStackTrace();
		}//end of main catch

	}//end of main method

}//end of class
