from pydantic import BaseModel

class Inspect(BaseModel):
    # Input state: raw_csv_path: str
    # Output state: 
    data_profile: dict

class Plan(BaseModel):
    # Input state: data_profile: dict
    # Output state:
    cleaning_plan: str

class GenerateCode(BaseModel):
    # Input state: cleaning_plan: str
    # Output state: 
    generated_code: str

class ExecuteCode(BaseModel):
    # Input state: generated_code: str
    # Output state: 
    error: str
    cleaned_csv_path: str

class FeatureEngineering(BaseModel):
    # Input state: cleaned_csv_path: str
    # Output state: 
    feature_engineering_plan: str

class Debug(BaseModel):
    # Input state: error: str
    # Output state:
    generated_code: str