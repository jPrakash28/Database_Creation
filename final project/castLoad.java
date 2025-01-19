import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class castLoad {

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
			
			sql = "DROP TABLE IF EXISTS casts";
            statement.execute(sql);
			
			csvFile = "/Users/josh/Documents/cast.csv";//Stores the file location of cast.csv to csvFile
			
			System.out.println("Creating Casts Table...");
			System.out.println();
			
			//The main sql code that creates a casts table
			sql = "CREATE TABLE IF NOT EXISTS casts (" 
					+ "movie_id FLOAT NOT NULL,"
					+ "cast_id FLOAT NOT NULL,"
					+ "cast_name VARCHAR(50),"
					+ "cast_gender FLOAT NOT NULL,"
					+ "cast_character VARCHAR(50),"
					+ "PRIMARY KEY (movie_id, cast_id),"
					+ "FOREIGN KEY (movie_id) REFERENCES movies(movie_id))";
			
			statement.execute(sql);
			
			buffReader = new BufferedReader(new FileReader(csvFile));//reads the file
			
			//while loop to keep adding information from the file to cast table
			while((fileLine = buffReader.readLine()) != null) {
				
				int i = -1;
				String[] castToken = fileLine.split(csvSplit);
				
				sql = "INSERT INTO casts VALUES (?, ?, ?, ?, ?)";//sql command to store the information from file to the table
				PreparedStatement prepStatement = connect.prepareStatement(sql);
				
				prepStatement.clearParameters();
				
				prepStatement.setFloat(1, Float.parseFloat(castToken[++i]));//adds movie id
				prepStatement.setFloat(2, Float.parseFloat(castToken[++i]));//adds cast id
				prepStatement.setString(3, castToken[++i]);//adds cast name
				prepStatement.setFloat(4,  Float.parseFloat(castToken[++i]));//adds cast gender
				//prepStatement.setFloat(5, Float.parseFloat(castToken[++i]));//adds cast id
				i++;
				prepStatement.setString(5, castToken[++i]);//adds cast character
				
				try {
					prepStatement.executeUpdate();//executes the above code 
				}//end of try
				catch(Exception e) {
				}//end of inner catch
				
				prepStatement.close();
				
			}//ends while loop
				
			System.out.println("Casts Table Created");
			System.out.println();
			
		}//ends main try
		catch(Exception e) {
			System.out.println(fileLine);
			System.out.println("SQL Exception");
			e.printStackTrace();
		}//ends catch

	}//ends main method

}//ends class
