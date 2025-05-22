# Amazon Reviews Sentiment Analysis

This project provides a real-time sentiment analysis pipeline for Amazon product reviews using Apache Kafka, Apache Spark Streaming, and MongoDB. It streams Amazon review data to Kafka, processes and predicts sentiment with Spark, and stores the results in MongoDB for further analysis or visualization.

## Features

\- Streams Amazon review data to a Kafka topic.  
\- Consumes reviews from Kafka using Spark Structured Streaming.  
\- Predicts sentiment using a pre-trained machine learning model.  
\- Stores enriched review data with sentiment in MongoDB.

## Project Structure

\- `kafka_producer/`: Scripts to stream review data to Kafka.  
\- `spark_streaming/`: Spark Streaming application for sentiment analysis.  
\- `models/`: Pre-trained sentiment model and vectorizer.  
\- `utils/`: Preprocessing and MongoDB utility functions.

## Prerequisites

\- Docker & Docker Compose  
\- Python 3.8+  
\- Apache Kafka  
\- Apache Spark  
\- MongoDB

## Quick Start

### Run with Docker Compose

1. **Clone the repository:**
   ```sh
   git clone https://github.com/Youssefzrr/amazon-reviews-sentiment.git
   cd amazon-reviews-sentiment
   ```

2. **Build and start the services:**
   ```sh
   docker-compose build
   docker-compose up -d
   ```

3. **Check MongoDB for results:**
   ```sh
   docker exec -it mongodb mongo
   use amazon_reviews
   db.reviews.find().pretty()
   ```

4. **Stop the services:**
   ```sh
   docker-compose down
    ```
## Configuration
- **Kafka Configuration:** Modify `kafka_producer/config.py` to set the Kafka broker address and topic name.
- **MongoDB Configuration:** Modify `spark_streaming/config.py` to set the MongoDB connection string and database/collection names.
- **Model Configuration:** Ensure the pre-trained model and vectorizer are correctly specified in `models/model.py`.
- **Review Data:** The producer script generates random reviews. You can modify the `kafka_producer/producer.py` script to customize the review data or use a different source.
- **Spark Configuration:** Adjust the Spark configuration in `spark_streaming/streaming_app.py` as needed for your environment.
- **Docker Configuration:** The `docker-compose.yml` file sets up Kafka, Zookeeper, and MongoDB. You can modify the configuration to suit your environment or add additional services as needed.
- **Dependencies:** Ensure all required Python packages are installed. You can use `pip install -r requirements.txt` to install the necessary packages.
- **Environment Variables:** If you want to use environment variables for configuration, you can set them in your shell or use a `.env` file with the `dotenv` package. Modify the code to read from environment variables as needed.
- **Logging:** The Spark application uses the `logging` module for logging. You can adjust the logging level and format in `spark_streaming/streaming_app.py`.
- **Error Handling:** The Spark application includes basic error handling. You can enhance it by adding more specific error handling for different scenarios, such as connection errors or data processing errors.
- **Testing:** You can add unit tests for the producer and Spark application to ensure the functionality works as expected. Use a testing framework like `unittest` or `pytest` for this purpose.
- **Documentation:** Consider adding more detailed documentation for each component of the project, including usage examples, configuration options, and troubleshooting tips. This will help users understand how to set up and use the project effectively.
- **Performance Tuning:** Depending on the volume of data and the complexity of the model, you may need to tune the performance of the Spark application. This can include adjusting the number of partitions, memory settings, and other Spark configurations.
- **Security:** If you plan to deploy this application in a production environment, consider implementing security measures such as authentication and encryption for Kafka and MongoDB connections. You can use SSL/TLS for secure communication and authentication mechanisms provided by Kafka and MongoDB.
- **Monitoring:** Set up monitoring for Kafka, Spark, and MongoDB to track performance and resource usage. You can use tools like Prometheus and Grafana for monitoring and alerting.
- **Scaling:** If you need to handle a large volume of data, consider deploying Kafka and Spark in a distributed environment. You can use Kubernetes or Docker Swarm for orchestration and scaling.
- **Deployment:** For production deployment, consider using a CI/CD pipeline to automate the build and deployment process. You can use tools like GitHub Actions, Jenkins, or GitLab CI/CD for this purpose.
- **License:** This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
- **Contributing:** Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.