FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create models directory with proper permissions
RUN mkdir -p /app/models_store && chmod 755 /app/models_store

# Expose port
EXPOSE 8001

# Run the application
CMD ["python", "main.py"]
