FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your code
COPY . .

# The port your app listens on inside the container
EXPOSE 8000

# Start the API
CMD ["uvicorn", "api_main:app", "--host", "0.0.0.0", "--port", "8000"]
