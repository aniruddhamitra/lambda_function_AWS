import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class S3ToDbLambda implements RequestHandler<S3Event, String> {

    private static final String BUCKET_NAME = "your-bucket-name";
    private static final String FILE_KEY = "your-file-key.csv";
    private static final String DB_URL = "jdbc:mysql://your-db-url:3306/your-db-name";
    private static final String DB_USER = "your-db-username";
    private static final String DB_PASSWORD = "your-db-password";

    @Override
    public String handleRequest(S3Event event, Context context) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3Client.getObject(BUCKET_NAME, FILE_KEY);
        S3ObjectInputStream s3InputStream = s3Object.getObjectContent();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3InputStream));
             CSVReader csvReader = new CSVReader(reader);
             Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {

            String[] nextLine;
            while ((nextLine = csvReader.readNext()) != null) {
                insertIntoDatabase(connection, nextLine);
            }

        } catch (Exception e) {
            context.getLogger().log("Error: " + e.getMessage());
            return "Error: " + e.getMessage();
        }

        return "Success";
    }

    private void insertIntoDatabase(Connection connection, String[] data) throws SQLException {
        String sql = "INSERT INTO your_table_name (column1, column2, column3) VALUES (?, ?, ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, data[0]);
            preparedStatement.setString(2, data[1]);
            preparedStatement.setString(3, data[2]);
            preparedStatement.executeUpdate();
        }
    }
}