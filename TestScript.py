from time import sleep
import sys

print("This is my file to demonstrate best practices.")

def process_data(data):
    print("Beginning data processing...")
    modified_data = data + " that has been modified"
    sleep(3)
    print("Data processing finished.")
    return modified_data

def main(args):
    data = args
    print("The input line is: " + data)
    modified_data = process_data(data)
    print(modified_data)

if __name__ == "__main__":
    para = str(sys.argv[1])
    main(para)




