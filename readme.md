# Technical Implementation Report: Twitch Network Representation System

## System Overview

The implemented system analyzes Twitch user network connections through a distributed architecture combining real-time stream processing with batch processing capabilities. The system follows a near-lambda architecture pattern, utilizing Apache Kafka for stream ingestion, Apache Spark for processing, HBase for storage, and a Flask/Streamlit web interface for visualization.

## Current Implementation

### Data Production Layer
The system implements a Kafka producer (TwitchNetworkProducer) that generates synthetic network edge data with the following characteristics:

- Generates 10 network edges per batch every minute
- Uses randomized user IDs (0-999) to create connections
- Implements graceful shutdown handling
- Uses GSON for JSON serialization
- Configurable broker settings and basic producer configurations
- Basic error handling for message delivery

### Stream Processing Layer
The Spark streaming application (TwitchNetworkProcessor) handles real-time data processing with these key features:

- Structured Streaming implementation reading from Kafka
- 10-second trigger intervals for batch processing
- Backpressure handling enabled
- Graceful shutdown support
- HBase integration for persistent storage
- First-level connection strength calculation
- Batch-level monitoring and logging
- Configurable parallelism (10 partitions)

### Storage Layer
The system utilizes HBase as the primary storage mechanism with:

- Two main tables: twitch_edges_hbase and twitch_features_hbase
- Integration with Spark through Hive tables
- First-level connections table for aggregated network data
- JDBC connection configuration for data access

### Web Interface Layer
The visualization system consists of two components:

#### Server-Side (Flask)
- RESTful API implementation
- Spark integration for data queries
- Connection strength aggregation
- CORS support for cross-origin requests
- Error handling and logging
- Query optimization for first-level connections

#### Client-Side (Streamlit)
- Interactive network visualization using Plotly
- SOCKS proxy support
- Response caching (1-hour TTL)
- Responsive grid layout for connected users
- Network size limitations (30 connections) for visualization
- Query parameter-based navigation

## Challenges and Current Limitations

The system's architectural challenges primarily stem from an incomplete lambda architecture implementation. Significant time was spent resolving complex integration issues between Spark, HBase, and Kafka, particularly around data consistency and stream processing configurations. These technical hurdles consumed valuable development time, leading to compromises in the overall architecture. The current setup lacks robust strategies for merging real-time and batch views, while the speed layer requires more sophisticated optimization. Performance bottlenecks exist due to fixed partition settings and a single Spark session handling all queries. The visualization system faces limitations when processing larger networks. Data management requires enhancement – basic validation exists, but comprehensive error recovery mechanisms and proper strategies for managing old or redundant data are absent.

## Recommended Improvements

The architecture requires strengthening through implementation of proper batch and serving layers, alongside robust data validation and versioning systems. The initial focus on getting the basic streaming pipeline functional meant these components were deprioritized. Performance improvements should include dynamic partition configuration and connection pooling implementation to handle larger datasets efficiently. A monitoring system with metrics collection and failover mechanisms would ensure stable operation. Feature enhancements such as user authentication, real-time network updates, and advanced analytics would increase system utility and security. These improvements, while important, were outside the scope of the initial implementation due to time constraints and the complexity of the existing components.

## Technical Debt Areas

Technical debt accumulated during the rapid development phase spans three main areas. The code organization requires restructuring – concerns lack proper separation, configuration needs externalization, and hardcoded values exist throughout the codebase. The focus on delivering core functionality resulted in minimal testing coverage – both unit and integration tests are missing, and error scenarios lack proper coverage. Documentation requires enhancement – API documentation, deployment guides, and performance tuning documentation need development to support future maintenance and development. These gaps emerged from prioritizing core functionality over development best practices during the initial implementation phase.


## Conclusion

The current implementation provides a solid foundation for Twitch network analysis but requires significant enhancements to achieve a pure lambda architecture. The system successfully demonstrates real-time processing capabilities but needs additional components for true batch processing and view merging. Future improvements should focus on scalability, reliability, and maintaining separate batch and speed layers while ensuring data consistency and system performance.

