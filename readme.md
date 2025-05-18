# 📬 Notification Service

A lightweight Python + Flask-based Notification Service that supports Email, SMS, and In-App notifications.  
Messages are processed asynchronously using RabbitMQ and stored in MongoDB.  
Includes retry logic with Dead Letter Exchange (DLX) and a background queue consumer.

---

## 🚀 Features

- **POST `/notifications`** → Queue a notification asynchronously  
- **GET `/users/<user_id>/notifications`** → Fetch user's notifications from MongoDB  
- RabbitMQ queue for async message processing  
- Retry logic via DLX (Dead Letter Exchange) with delayed retries  
- MongoDB persistence for notification history  
- Minimalistic logging and error handling  
- Supports Email, SMS, and In-App notification types  
- Simulated sending (no real emails/SMS)  

---

## 📦 Tech Stack

- Python 3.10+  
- Flask  
- RabbitMQ  
- MongoDB  
- [Pika](https://pika.readthedocs.io/) (RabbitMQ client)  
- [PyMongo](https://pymongo.readthedocs.io/) (MongoDB client)  

---

## 🧰 Setup Instructions

### 1️⃣ Clone the repository

```bash
git clone https://github.com/yourusername/notification-service.git
cd notification-service
```

### 2️⃣ Install Python dependencies

```bash
pip install flask pika pymongo
```

*Optionally save to `requirements.txt` for future use.*

### 3️⃣ Install & Run RabbitMQ

#### 🛠 Option 1: Docker (Recommended)

```bash
docker run -d --hostname rabbit --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3-management
```

- RabbitMQ UI: [http://localhost:15672](http://localhost:15672)  
- Default login: `guest` / `guest`

#### 🛠 Option 2: Manual Install

Download & install from [RabbitMQ official site](https://www.rabbitmq.com/download.html).  
Start RabbitMQ server after installation.

---

### 4️⃣ Install & Run MongoDB

#### 🛠 Option 1: Docker

```bash
docker run -d --name mongodb -p 27017:27017 mongo
```

#### 🛠 Option 2: Install Locally

Download & install from [MongoDB Community Server](https://www.mongodb.com/try/download/community).  
Start MongoDB service.

---

### 5️⃣ Run the Flask App

```bash
python app.py
```

You should see logs like:

```
MongoDB connect success
RabbitMQ connect work!
Background worker start eat!
```

The Flask server will run at:  
[http://localhost:3000](http://localhost:3000)

---

## 📮 API Endpoints

### 1. POST `/notifications`

Queue a notification asynchronously.

**Request Body:**

```json
{
  "user_id": "123",
  "type": "sms",
  "message": "Hello from Pepsales!"
}
```

**Response:**

```json
{
  "status": "Notify in queue now"
}
```

---

### 2. GET `/users/<user_id>/notifications`

Fetch all notifications for a user.

**Example:**

```
GET /users/123/notifications
```

**Response:**

```json
{
  "user_id": "123",
  "notifications": [
    {
      "_id": "65a4...",
      "type": "sms",
      "message": "Hello from Pepsales!",
      "timestamp": 1716000000.0
    }
  ]
}
```

---

## 🔁 Retry Logic

- On processing failure, messages are moved to a retry queue via Dead Letter Exchange (DLX)  
- Each retry is delayed by 60 seconds  
- Maximum of 3 retries before the message is dropped  

---

## 💡 Assumptions

- Actual notification sending (email, SMS) is **simulated** — no external API calls  
- MongoDB and RabbitMQ assumed to run locally on default ports  
- Temporary use of `eval()` for decoding RabbitMQ messages (replace with safer parsing in production)  

---

## ✅ Assignment Checklist

- [x] Flask POST and GET endpoints  
- [x] Support for Email, SMS, In-App notifications  
- [x] RabbitMQ async queue with retry using DLX  
- [x] MongoDB persistence for notifications  
- [x] Full setup guide included  

---

Feel free to open issues or contribute!  
Happy notifying! 🚀