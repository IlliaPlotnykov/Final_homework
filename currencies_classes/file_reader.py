import pandas as pd

class FileReader:
    @staticmethod
    def get_currency_list(): #Create Currencies List
        df = ["USD", "EUR", "PLN","GBP"]
        return df
        #df = pd.read_excel('D:\\Programs\\all_python_projects\\Сurrency_Exchange\\config\\list_currencies.xlsx') #Лист валют з ексель файлу
        #return df['currencies'].tolist()

