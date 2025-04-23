# Running the MLflow-Based Image Classification System

Based on the provided code files, this system is designed to run an image classification service with MLflow for model tracking, versioning, and monitoring. Here's a step-by-step guide to get it running:

## Prerequisites

You'll need:
- Python with the packages listed in `requirements.txt`
- MySQL server 
- The pre-trained ResNet18 weights file (`models/resnet18_weights.pth`)
- An `imagenet_classes.txt` file in the models directory

## Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

## Step 2: Setup MySQL Database

The system expects a MySQL database named `mlflow`. You can create it using the provided SQL script:

```bash
CREATE DATABASE IF NOT EXISTS mlflow;
```

## Step 3: Start the MLflow Tracking Server

Start MLflow with MySQL as the backend storage:

```bash
python start_mlflow_local.py
```

This will start the MLflow server at http://localhost:5000.

## Step 4: Register Your Initial Model

Before starting the API, you need to register the base model with MLflow:

```bash
python -m scripts.setup_mlflow
```

This will:
- Connect to the MLflow server
- Register the existing ResNet18 model
- Mark it as "Production" in the MLflow model registry

## Step 5: Start the FastAPI Service

After the model is registered, you can start the FastAPI service:

```bash
uvicorn app.main:app --reload
```

This will start the API at http://localhost:8000. You can then:
- Test the service at http://localhost:8000/health
- Upload images for classification at http://localhost:8000/predict-image

## Optional: Fine-tune the Model

If you have a dataset you'd like to use for fine-tuning:

```bash
python -m scripts.finetune --data_dir ./data --epochs 1 --batch_size 32 --lr 0.01
```

Your data directory should have this structure:
```
data_dir/
├── train/
│   ├── class1/
│   │   ├── image1.jpg
│   │   └── ...
│   ├── class2/
│   │   └── ...
└── val/
    ├── class1/
    │   └── ...
    └── class2/
        └── ...
```

## System Overview

The system implements an end-to-end ML workflow:
1. MLflow tracks model versions, parameters, and metrics
2. FastAPI provides the prediction endpoint
3. The system tracks data drift for model monitoring
4. You can fine-tune on new data and automatically register new models

All predictions are logged to MLflow, making it easy to track model performance over time.