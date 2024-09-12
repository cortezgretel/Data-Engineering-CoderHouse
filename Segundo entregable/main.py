from extract import extract
from transform import transform
from load import load

def main():
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    print("Transformed Data")
    print(transformed_data)
    load(transformed_data)

if __name__ == "__main__":
    main()



