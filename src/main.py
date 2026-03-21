from extract import extract_all
from transforme import transform_all
from load import load_data


def run_etl() -> None:
    print("[main] starting etl process")
    raw_data = extract_all()
    transformed_data = transform_all(raw_data)
    load_data(transformed_data)
    print("[main] etl process completed")
    
    
if __name__ == "__main__":
    run_etl()