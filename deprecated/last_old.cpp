#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <variant>
#include "csv.hpp"

/*
Major problems solved
1- auto detect the datatype
2- create a binary mask filter (not finished), support for (or, and) operations in filter, we can use a binary mask instead of a vector of a binary mask

3- create a copy based on the binary mask (not finished)
4- create a value count function


extra todos:
    support subscript
    support and, or , not operations for binary mask (currently it is only "and" operation)
    there are many if statements for each datatype in (read_csv, append, and potentially get_filter)
*/


/*
    ENUM datatypes that was already defined in csv.hpp


        enum class DataType {
        UNKNOWN = -1,
        CSV_NULL,   < Empty string 
        CSV_STRING, < Non-numeric string 
        CSV_INT8,   < 8-bit integer 
        CSV_INT16,  < 16-bit integer (short on MSVC/GCC) 
        CSV_INT32,  < 32-bit integer (int on MSVC/GCC) 
        CSV_INT64,  < 64-bit integer (long long on MSVC/GCC) 
        CSV_BIGINT, < Value too big to fit in a 64-bit in 
        CSV_DOUBLE  < Floating point value 
    };
 */
using namespace std;

// for a flexible datatype, this datatype will be the default for storing the column, 
// todo: add support of bool
using Column = variant<vector<int>, vector<double>, vector<string>>;
using CellValue = variant<int, double, string>;

// declare this class, since it is used in DataFrame

class DataFrame {
private:
    unordered_map<string, Column> data;
    vector<string> columns_names;
    vector<int> columns_dtypes;

    size_t num_rows = 0;

public:
    // initialize an empty dataframe with an existing scheme
    DataFrame(vector<string>* col_names = nullptr, vector<int>* col_dtypes = nullptr) {

        // if you already have a schema with the header and the datatypes, initialize it
        if (col_names && col_dtypes) {

            columns_names = *col_names;
            columns_dtypes = *col_dtypes;

            for (size_t i = 0; i < columns_names.size(); i++) {

                const string& name = columns_names[i];
                switch (static_cast<int>(columns_dtypes[i])) {
                    case static_cast<int>(csv::DataType::CSV_STRING):
                        data[name] = vector<string>();
                        break;

                    case static_cast<int>(csv::DataType::CSV_DOUBLE):
                        data[name] = vector<double>();
                        break;

                    default: // all integer types
                        data[name] = vector<int>();
                        break;
                }
            }
        }
    }

    // AI-Generated: helper function to print the schema of the dataframe
    void print_column_info() {
        for (const auto& name : columns_names) {
            cout << "   Column: " << name << " | Type: ";
            const auto& col = data[name];
            if (holds_alternative<vector<int>>(col)) cout << "int";
            else if (holds_alternative<vector<double>>(col)) cout << "double";
            else cout << "string";
            cout << endl;
        }
    }
    
    // AI-Generated: helper function to print the dataframe
    void print(int count = -1) {
        int limit = (count == -1) ? num_rows : min(count, (int)num_rows);
        for (const auto& name : columns_names) cout << name << "\t";
        cout << "\n-------------------------------\n";
        for (int i = 0; i < limit; ++i) {
            for (const auto& name : columns_names) {
                visit([i](auto& v) { cout << v[i] << "\t"; }, data[name]);
            }
            cout << "\n";
        }
    }

    // load and read the csv
    void read_csv(string directory) {
        // We first need to detect the datatypes ---

        // CSVStat stat() Opens the CSV file and reads through the entire file once. For every cell it sees, it records what type it was (int, double, string, null). By the time this line finishes, the whole file has been scanned. All that info is stored inside stat.
        csv::CSVStat stat(directory);
        columns_names = stat.get_col_names();
        columns_dtypes = *(new vector<int>);
        vector<csv::CSVStat::TypeCount> raw_dtypes = stat.get_dtypes();

        // Promote: if any cell is double, column is double; if any is string, column is string
        vector<string> int_cols, double_cols, string_cols;
        for (size_t i = 0; i < columns_names.size(); i++) {
            csv::CSVStat::TypeCount& col = raw_dtypes[i];
            string& name = columns_names[i];

            // if there is at least one string, it must be a string
            if (col[csv::DataType::CSV_STRING]) {
                data[name] = vector<string>();
                string_cols.push_back(name);
                columns_dtypes.push_back(2);

            // if there aren't any strings, and there is at least one float, it must be a double
            } else if (col[csv::DataType::CSV_DOUBLE]) {
                data[name] = vector<double>();
                double_cols.push_back(name);
                columns_dtypes.push_back(1);

            // otherwise it is int
            } else {
                data[name] = vector<int>();
                int_cols.push_back(name);
                columns_dtypes.push_back(0);
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
        cout << "Loaded " << num_rows << " rows across " << columns_names.size() << " columns.\n";
    }


    // add a new row to the existing dataframe
    void append(const vector<CellValue>& row) {
        // making sure the new row matches the number of columns in the dataframe
        if (row.size() != columns_names.size()) {
            cout << "Error: Row size (" << row.size() << ") does not match column count (" << columns_names.size() << ")." << "\n";
            return;
        }

        // Iterating over each value in the row and push it to the correct column vector
        for (size_t i = 0; i < columns_names.size(); i++) {
            const string& name = columns_names[i];
            const CellValue& val = row[i];

            // Match the column datatype based on read_csv() logic (0=int, 1=double, 2=string)
            if (columns_dtypes[i] == 2) { 
                get<vector<string>>(data[name]).push_back(get<string>(val));
            } 
            else if (columns_dtypes[i] == 1) { 
                get<vector<double>>(data[name]).push_back(get<double>(val));
            } 
            else { 
                get<vector<int>>(data[name]).push_back(get<int>(val));
            }
        }
        
        // 3. Update the total row count
        num_rows++;
        cout << "Row appended successfully. Total rows: " << num_rows << endl;
    }

    vector<CellValue> get_row(size_t index) {
        vector<CellValue> row;
        // Handle negative indexing (circular indexing)
        // If index is -1, it becomes (num_rows - 1), which is the last row.
        int actual_index = index;
        if (index < 0) {
            actual_index = static_cast<int>(num_rows) + index;
        }

        // Safety check for out-of-bounds
        if (actual_index < 0 || actual_index >= static_cast<int>(num_rows)) {
            cout << "Error: Row index (" << index << ") is out of bounds." << endl;
            return row;
        }
        // Extracting the value from each column at the given index
        for (size_t i = 0; i < columns_names.size(); i++) {
            const string& name = columns_names[i];

            // Extracting based on the datatype we mapped (0=int, 1=double, 2=string)
            if (columns_dtypes[i] == 2) { 
                row.push_back(get<vector<string>>(data[name])[index]);
            } 
            else if (columns_dtypes[i] == 1) { 
                row.push_back(get<vector<double>>(data[name])[index]);
            } 
            else { 
                row.push_back(get<vector<int>>(data[name])[index]);
            }
        }

        return row;
    }

    // a function that returns a binary mask, this binary mask can be used to combine multiple queries. TODO: this binary mask shouldn't be a vector
    template<typename T>                                                                                                                                                                                              
    vector<bool>* get_filter(string column_name, T value, vector<bool>*binray_filter = nullptr){
        
        // if the binray_filter is null, we initialize a new binray_filter with all zeros
        if (binray_filter == nullptr) binray_filter = new vector<bool>(num_rows, 1);

        vector<T> current_column = get<vector<T>>(data[column_name]);

        for (int i =0; i < current_column.size(); i++){

            if (current_column[i] != value){
                // flag this index in the binary binray_filter with 1
                (*binray_filter)[i] = 0;
            }
        }
        return binray_filter;
    }

    // return a new dataframe based on the binary_filter
    template<typename T>                                                                                                                                                                                              
    DataFrame* where(vector<bool>* binary_filter){
        
        // create a new dataframe that stores the new rows
        DataFrame* new_df = new DataFrame(&columns_names, &columns_dtypes);
        
        // iterate over the row_count
        for(int i = 0; i < num_rows; i++){
            // skip if it is not included in teh filter
            if (!(*binary_filter)[i]){
                new_df->append(this->get_row(i));
            }            
            }

        return new_df;
    }
    
    // to implement, get a map of the unique values of a column with their counts
    // value
    // template<typename T>                                                                                                                                                                                                  
    // unordered_map<T, int>* value_counts(string column_name, vector<bool>*binray_filter = nullptr){

    //     // to make the code clean, if the binary filter is a nullptr, we create a new
    //     if (binray_filter == nullptr) binray_filter = new vector<bool>(num_rows, 1);
        
        
    //     // get the column vector
    //     vector<T> current_column = get<vector<T>>(data[column_name]);


    //     unordered_map<>

    // }

};



int main() {
 
    DataFrame df;
    df.read_csv("./Soccer Performance Data/Soccer Performance Data.csv");
    
    cout << "\n Data Frame Loaded\n" << endl;


    df.append(df.get_row(1));

    // Print the first 10 rows
    df.print(10);

    return 0;
}