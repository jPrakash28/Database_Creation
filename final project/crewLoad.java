import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class crewLoad {
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
			
			sql = "DROP TABLE IF EXISTS crew";
            statement.execute(sql);
			
			csvFile = "/Users/josh/Documents/crew.csv";//Stores the file location of crew.csv to csvFile
			
			System.out.println("Creating Crew Table...");
			System.out.println();
			
			//The main sql code that creates a crew table
			sql = "CREATE TABLE IF NOT EXISTS crew ("
					+ "movie_id FLOAT NOT NULL,"
					+ "crew_id FLOAT NOT NULL,"
					+ "crew_name VARCHAR(50),"
					+ "crew_gender FLOAT NOT NULL,"
					+ "crew_department VARCHAR(50),"
					+ "crew_job VARCHAR(50),"
					+ "FOREIGN KEY (movie_id) REFERENCES movies(movie_id),"
					+ "PRIMARY KEY (movie_id, crew_id))";
			
			statement.execute(sql);//creates the table in mysql
			
			buffReader = new BufferedReader(new FileReader(csvFile));//reads the file
			
			//while loop to keep adding information from the file to crew table
			while((fileLine = buffReader.readLine()) != null) {
				
				int i = -1;
				String[] crewToken = fileLine.split(csvSplit);
				
				sql = "INSERT INTO crew VALUES (?, ?, ?, ?, ?, ?)";//sql command to store the information from file to the table
				PreparedStatement prepStatement = connect.prepareStatement(sql);
				
				prepStatement.clearParameters();
				
				prepStatement.setFloat(1, Float.parseFloat(crewToken[++i]));//adds movie id 
				prepStatement.setFloat(2, Float.parseFloat(crewToken[++i]));//adds crew id
				prepStatement.setString(3,  crewToken[++i]);//adds crew name
				prepStatement.setFloat(4,  Float.parseFloat(crewToken[++i]));//adds crew gender
				prepStatement.setString(5, crewToken[++i]);//adds crew department
				prepStatement.setString(6,  crewToken[++i]);//adds crew job
				
				try {
					prepStatement.executeUpdate();//executes the above code 
				}//end of try
				catch(Exception e) {
				}//end of inner catch
				
				prepStatement.close();
				
				
			}//end of while
			
			System.out.println("Crew Table Created");
			System.out.println();
			
		}
		//catch statement which catches the SQL Exceptions
		catch(Exception e) {
			System.out.println(fileLine);
			System.out.println("SQL Exception");
			e.printStackTrace();
		}//end of main catch

	}//end of main method

}//end of class
