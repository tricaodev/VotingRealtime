# System Architecture
![system_architecture](https://github.com/user-attachments/assets/f15ca6df-4a7d-4c7b-9900-c83261393869)

# How to run app
## 1. Build system architecture
```docker-compose up -d```
## 2. Install packages
```pip install -r requirements.txt```
## 3. Generate candidate data and voter data, then produce voter data to kafka
```python main.py```
## 4. Generate vote data of voter for candidate, then produce it to kafka
```python voting.py```
## 5. Enrich data and streaming it to kafka by spark job
* ```docker exec -it spark-master bash```
* ```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 jobs/streaming_data.py```
## 6. Run streamlit app
```streamlit run streamlit_app.py```
## 4. Review dashboard
* Launch to http://localhost:8501/ and review streamlit dashboard
<img width="1919" height="987" alt="image" src="https://github.com/user-attachments/assets/335a1dec-54b9-41ad-a55f-b6d597b6349b" />
