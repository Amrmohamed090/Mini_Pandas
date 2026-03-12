#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <variant>
#include "csv.hpp"


/*
todo list:

support for (or, and) operations in filter, we can use a binary mask instead of a vector of a binary mask


*/
using namespace std;

// for a flexible datatype, this datatype will be the default for storing the column, 
using Column = variant<vector<int>, vector<double>, vector<string>>;


class DataFrame {
private:
    unordered_map<string, Column> data;
    vector<string> column_names;
    size_t num_rows = 0;

public:

    void print_column_info() {
        for (const auto& name : column_names) {
            cout << "   Column: " << name << " | Type: ";
            const auto& col = data[name];
            if (holds_alternative<vector<int>>(col)) cout << "int";
            else if (holds_alternative<vector<double>>(col)) cout << "double";
            else cout << "string";
            cout << endl;
        }
    }

    void read_csv(string directory) {
        // We first need to detect the datatypes ---

        // CSVStat stat() Opens the CSV file and reads through the entire file once. For every cell it sees, it records what type it was (int, double, string, null). By the time this line finishes, the whole file has been scanned. All that info is stored inside stat.
        csv::CSVStat stat(directory);
        column_names = stat.get_col_names();
        vector<csv::CSVStat::TypeCount> raw_dtypes = stat.get_dtypes();

        // Promote: if any cell is double, column is double; if any is string, column is string
        vector<string> int_cols, double_cols, string_cols;
        for (size_t i = 0; i < column_names.size(); i++) {
            csv::CSVStat::TypeCount& col = raw_dtypes[i];
            string& name = column_names[i];

            // if there is at least one string, it must be a string
            if (col[csv::DataType::CSV_STRING]) {
                data[name] = vector<string>();
                string_cols.push_back(name);

            // if there aren't any strings, and there is at least one float, it must be a double
            } else if (col[csv::DataType::CSV_DOUBLE]) {
                data[name] = vector<double>();
                double_cols.push_back(name);

            // otherwise it is int
            } else {
                data[name] = vector<int>();
                int_cols.push_back(name);
            }
        }

        // We need to load the Data, unfortunately, I have to iterate over the rows in this data_reader
        // since CSV is stored line by line. that is the trade-off we need to do if we are going with a columnar format.
        // However, since we already know the type of each column from Pass 1, we pre-sorted them into
        // int_cols, double_cols, string_cols, so no type checking happens here at all.
        csv::CSVReader data_reader(directory);
        for (csv::CSVRow& row : data_reader) {
            for (string& name : int_cols)
                // data[name] holds a type of "variant", so to extract the actual value of a variant we need to use get<T>(variant)
                get<vector<int>>(data[name]).push_back(row[name].get<int>());
            for (string& name : double_cols)
                get<vector<double>>(data[name]).push_back(row[name].get<double>());
            for (string& name : string_cols)
                get<vector<string>>(data[name]).push_back(row[name].get<string>());
            num_rows++;
        }
        print_column_info();
        cout << "Loaded " << num_rows << " rows across " << column_names.size() << " columns.\n";
    }


    // a function that returns a binary mask, this binary mask can be used to combine multiple queries. TODO: this binary mask shouldn't be a vector
    template<typename T>                                                                                                                                                                                              
    vector<size_t>* get_filter(string column_name, T value, vector<size_t>*binray_filter = nullptr){
        
        // if the binray_filter is null, we initialize a new binray_filter with all zeros
        if (binray_filter == nullptr) binray_filter = new vector<size_t>(num_rows, 1);

        vector<T> current_column = get<vector<T>>(data[column_name]);

        for (int i =0; i < current_column.size(); i++){

            if (current_column[i] != value){
                // flag this index in the binary binray_filter with 1
                (*binray_filter)[i] = 0;
            }
        }
        return binray_filter;
    }

    // the difference between this function and the previous one, 
    template<typename T>                                                                                                                                                                                              
    vector<T>* where(string column_name, T value, vector<size_t>* binary_filter){
        

    }
    
    // to implement, get a map of the unique values of a column with their counts
    // value
    template<typename T>                                                                                                                                                                                                  
    unordered_map<T, int>* value_counts(string column_name, vector<size_t>*binray_filter = nullptr){

        // to make the code clean, if the binary filter is a nullptr, we create a new
        if (binray_filter == nullptr) binray_filter = new vector<size_t>(num_rows, 1);
        
        
        // get the column vector
        vector<T> current_column = get<vector<T>>(data[column_name]);


        unordered_map<>

    }

};

int main() {
 
    DataFrame df;
    df.read_csv("./Soccer Performance Data/Soccer Performance Data.csv");
    
    cout << "\n Data Frame Loaded\n" << endl;

    vector<size_t>* filter = df.get_filter("Name", string("Jack"));
    
    return 0;
}