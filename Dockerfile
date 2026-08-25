# Use an official lightweight Python runtime as a parent image
FROM python:3.12-slim

# Set system environment variables to prevent Python from writing pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install the required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application source code into the container
COPY . .

# Expose the port that Streamlit uses (default is 8501)
EXPOSE 8501

# Command to execute the Streamlit UI directly inside the container as a native Python module in Shell Form
CMD python -m streamlit run app_ui.py --server.port=8501 --server.address=0.0.0.0 --server.fileWatcherType=none