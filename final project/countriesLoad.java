import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class countriesLoad {

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
			
			sql = "DROP TABLE IF EXISTS countries";
            statement.execute(sql);
			
			csvFile = "/Users/josh/Documents/countries.csv";//Stores the file location of countries.csv to csvFile
			
			System.out.println("Creating Countries Table...");
			System.out.println();
			
			//The main sql code that creates a countries table
			sql = "CREATE TABLE IF NOT EXISTS countries ("
					+ "movie_id FLOAT NOT NULL,"
					+ "countries_abreviate VARCHAR(2),"
					+ "countries_name VARCHAR(50),"
					+ "PRIMARY KEY (movie_id, countries_abreviate),"
					+ "FOREIGN KEY (movie_id) REFERENCES movies(movie_id))";
			
			statement.execute(sql);
			
			buffReader = new BufferedReader(new FileReader(csvFile));//reads the file
			
			//while loop to keep adding information from the file to countries table
			while((fileLine = buffReader.readLine()) != null) {
				
				int i = -1;
				String[] countriesToken = fileLine.split(csvSplit);
				
				sql = "INSERT INTO countries VALUES (?, ?, ?)";//sql command to store the information from file to the table
				PreparedStatement prepStatement = connect.prepareStatement(sql);
				
				prepStatement.clearParameters();
				
				prepStatement.setFloat(1, Float.parseFloat(countriesToken[++i]));//adds movie id
				prepStatement.setString(2, countriesToken[++i]);//adds countries abbreviation
				prepStatement.setString(3,  countriesToken[++i]);//adds countries name
				
				try {
					prepStatement.executeUpdate();//executes the above code 
				}//end of try
				catch(Exception e) {
				}//end of inner catch
				
				prepStatement.close();
				
			}//end of while
			
			System.out.println("Countries Table Created");
			System.out.println();
			
		}
		catch(Exception e) {
			System.out.println(fileLine);
			System.out.println("SQL Exception");
			e.printStackTrace();
		}//end of catch

	}//end of main method

}//end of class
