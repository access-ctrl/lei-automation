# Use the official Playwright image which has all browser dependencies pre-installed
FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

# Set working directory
WORKDIR /app

# Ensure logs are printed instantly
ENV PYTHONUNBUFFERED=1

# Copy requirements and install
COPY requirements.txt .
RUN apt-get update && apt-get install -y xvfb && pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (Chromium)
RUN playwright install chromium

# Copy the rest of the application
COPY . .

# Expose the FastAPI port
EXPOSE 8000

# Start the API server
CMD ["python", "api_server.py"]
