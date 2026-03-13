#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <unordered_map>
#include <variant>
#include <algorithm>
#include <bitset>
#include "csv.hpp"

using namespace std;



/* BINARY MASK CONFIG */
// the maximum size for the filter mask
const size_t MAX_ROWS = 1000;


//define the mask type
using Mask = std::bitset<MAX_ROWS>;



using CellValue = variant<int, double, string>;

enum class DType { INT, DOUBLE, STRING };

// Class to store the column of the dataframe
template<typename T>
class Series {
private:
    vector<T> data;

public:
    Series() {}

    void push_back(T val) { data.push_back(val); }

    // to make the series subscripable, like thes series[i]
    T operator[](size_t i){ return data[i]; }

    // to return the full vector
    vector<T>& get_data()          { return data; }

    double mean() {
        if (data.empty()) { cout << "Error: mean() called on empty Series.\n"; return 0; }
        double sum = 0;
        for (T v : data) sum += v;
        return sum / data.size();
    }

    double median() {
        if (data.empty()) { cout << "Error: median() called on empty Series.\n"; return 0; }
        vector<T> sorted = data;
        sort(sorted.begin(), sorted.end());
        size_t n = sorted.size();
        return (n % 2 == 0) ? (sorted[n/2-1] + sorted[n/2]) / 2.0 : sorted[n/2];
    }

    double mode() {
        if (data.empty()) { cout << "Error: mode() called on empty Series.\n"; return 0; }
        unordered_map<T, int> counts;
        for (T v : data) counts[v]++;
        T mode_val = data[0];
        int max_count = 0;
        for (auto& [val, count] : counts)
            if (count > max_count) { max_count = count; mode_val = val; }
        return (double)mode_val;
    }
};

// a column in DataFrame is one of these three Series types
using AnySeries = variant<Series<int>, Series<double>, Series<string>>;


class DataFrame {
private:
    unordered_map<string, AnySeries> data;
    vector<string> columns_names;
    vector<DType>  columns_dtypes;
    size_t num_rows = 0;

public:
    // get a Series out of the map directly
    template<typename T>
    Series<T>& get_series(const string& name) {
        if (data.find(name) == data.end())
            throw runtime_error("Column not found: " + name);
        return get<Series<T>>(data[name]);
    }

    // constructor, if we need to initialize an empty dataframe with an existing scheme
    DataFrame(vector<string>* col_names = nullptr, vector<DType>* col_dtypes = nullptr) {

        // if you already have a schema with the header and the datatypes, initialize it
        if (col_names && col_dtypes) {
            columns_names = *col_names;
            columns_dtypes = *col_dtypes;

            for (size_t i = 0; i < columns_names.size(); i++) {
                const string& name = columns_names[i];
                if      (columns_dtypes[i] == DType::STRING) data[name] = Series<string>();
                else if (columns_dtypes[i] == DType::DOUBLE) data[name] = Series<double>();
                else                                         data[name] = Series<int>();
            }
        }
    }

    // helper function to print the schema of the dataframe
    void print_column_info() {
        for (size_t i = 0; i < columns_names.size(); i++) {
            cout << "   Column: " << columns_names[i] << " | Type: ";
            if      (columns_dtypes[i] == DType::STRING) cout << "string";
            else if (columns_dtypes[i] == DType::DOUBLE) cout << "double";
            else                                         cout << "int";
            cout << "\n";
        }
    }

    // load and read the csv
    void read_csv(string directory) {
        // We first need to detect the datatypes ---

        // CSVStat stat() Opens the CSV file and reads through the entire file once. For every cell it sees, it records what type it was (int, double, string, null). By the time this line finishes, the whole file has been scanned. All that info is stored inside stat.
        csv::CSVStat stat(directory);
        columns_names = stat.get_col_names();
        vector<csv::CSVStat::TypeCount> raw_dtypes = stat.get_dtypes();

        // Promote: if any cell is double, column is double; if any is string, column is string
        vector<string> int_cols, double_cols, string_cols;
        for (size_t i = 0; i < columns_names.size(); i++) {
            csv::CSVStat::TypeCount& col = raw_dtypes[i];
            string& name = columns_names[i];

            // if there is at least one string, it must be a string
            if (col[csv::DataType::CSV_STRING]) {
                data[name] = Series<string>();
                string_cols.push_back(name);
                columns_dtypes.push_back(DType::STRING);

            // if there aren't any strings, and there is at least one float, it must be a double
            } else if (col[csv::DataType::CSV_DOUBLE]) {
                data[name] = Series<double>();
                double_cols.push_back(name);
                columns_dtypes.push_back(DType::DOUBLE);

            // otherwise it is int
            } else {
                data[name] = Series<int>();
                int_cols.push_back(name);
                columns_dtypes.push_back(DType::INT);
            }
        }

        // We need to load the Data, unfortunately, I have to iterate over the rows in this data_reader
        // since CSV is stored line by line. that is the trade-off we need to do if we are going with a columnar format.
        // However, since we already know the type of each column from Pass 1, we pre-sorted them into
        // int_cols, double_cols, string_cols, so no type checking happens here at all.
        csv::CSVReader data_reader(directory);
        for (csv::CSVRow& row : data_reader) {
            for (string& name : int_cols)    get_series<int>(name).push_back(row[name].get<int>());
            for (string& name : double_cols) get_series<double>(name).push_back(row[name].get<double>());
            for (string& name : string_cols) get_series<string>(name).push_back(row[name].get<string>());
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

            // Match the column datatype based on read_csv() logic
            if (columns_dtypes[i] == DType::STRING) {
                if (!holds_alternative<string>(val)) { cout << "Error: type mismatch at column \"" << name << "\".\n"; return; }
                get_series<string>(name).push_back(get<string>(val));
            } else if (columns_dtypes[i] == DType::DOUBLE) {
                if (!holds_alternative<double>(val)) { cout << "Error: type mismatch at column \"" << name << "\".\n"; return; }
                get_series<double>(name).push_back(get<double>(val));
            } else {
                if (!holds_alternative<int>(val))    { cout << "Error: type mismatch at column \"" << name << "\".\n"; return; }
                get_series<int>(name).push_back(get<int>(val));
            }
        }

        // 3. Update the total row count
        num_rows++;
    }

    vector<CellValue> get_row(size_t index) {
        // initialize the row we will return
        vector<CellValue> row;

        // Safety check for out-of-bounds
        if (index >= num_rows) {
            cout << "Error: Row index (" << index << ") is out of bounds." << endl;
            return row;
        }

        // Extracting the value from each column at the given index
        for (size_t i = 0; i < columns_names.size(); i++) {
            const string& name = columns_names[i];
            if      (columns_dtypes[i] == DType::STRING) row.push_back(get_series<string>(name)[index]);
            else if (columns_dtypes[i] == DType::DOUBLE) row.push_back(get_series<double>(name)[index]);
            else                                         row.push_back(get_series<int>(name)[index]);
        }

        return row;
    }

    // const char* overload: converts "Jack" literals to string automatically so you don't need string("Jack")
    Mask filter(string column_name, const char* value, Mask* mask = nullptr) {
        return filter(column_name, string(value), mask);
    }


    // this function returns a binary mask, this mask is the main idea of the whole code
    template<typename T>
    Mask filter(string column_name, T value, Mask* mask = nullptr) {
        // if the mask is null, we initialize a new mask with all ones (everything passes)
        if (mask == nullptr) {
            mask = new Mask();
            mask->set();   // set all bits to 1
        }

        if (!holds_alternative<Series<T>>(data[column_name])) {
            cout << "Error: type mismatch for column \"" << column_name << "\".\n";
            return *mask;
        }

        Series<T>& current_column = get_series<T>(column_name);

        for (int i = 0; i < num_rows; i++) {
            if (current_column[i] != value)
                // set this bit to zero
                mask->reset(i);
        }
        return *mask;
    }

    // return a new dataframe based on the binary_filter, you need to use filter() before you use this function
    DataFrame where(Mask binary_filter) {
        // create a new dataframe that stores the new rows
        DataFrame new_df = DataFrame(&columns_names, &columns_dtypes);

        // iterate over the row_count and fill the new dataframe
        for (int i = 0; i < num_rows; i++) {
            if (binary_filter.test(i))
                new_df.append(this->get_row(i));
        }

        return new_df;
    }

    // get a map of the unique values of a column with their counts
    template<typename T>
    map<T, int> value_counts(string col_name) {
        map<T, int> counts;
        for (const T& val : get_series<T>(col_name).get_data())
            counts[val]++;
        return counts;
    }

};


int main() {
    DataFrame df;
    df.read_csv("./Soccer Performance Data/Soccer Performance Data.csv");

    cout << "\n Data Frame Loaded\n" << endl;

    // initialize some filters to reuse them
    Mask practice_session_mask = df.filter("Session_Type", "Practice");
    Mask game_session_mask = df.filter("Session_Type", "Game");

    // initialize a binary mask for each player
    map<string, Mask> players_mask_map;
    map<string, int> mp = df.value_counts<string>("Name"); // first lets get the unique players names

    // iterate over each player and fill its mask
    for (const auto& [name, frequency] : mp)
        players_mask_map[name] = df.filter("Name", name);


    cout<<"1. The mean maximum speed for each player during Practice sessions.\n\n";

        // for each unique player, get the average maximum speed
        for (const auto& [name, player_mask] : players_mask_map){
            DataFrame player = df.where(practice_session_mask & player_mask);
            cout <<"    " << name << " : " << player.get_series<double>("Max_Speed").mean() << "\n";
        }
        cout<<'\n';


    cout << "2. The mean maximum speed for each player Game sessions.\n\n";

        //similarly for the Game session
        for (const auto& [name, player_mask] : players_mask_map){
            DataFrame player = df.where(game_session_mask & player_mask);
            cout <<"    " << name << " : " << player.get_series<double>("Max_Speed").mean() << "\n";
        }
        cout<<'\n';

    cout << "3. The mean, median, and mode of sleep quality for each player.\n\n";

        for (const auto& [name, player_mask] : players_mask_map){
            // filter based on the player and get the sleep quality series
            Series<double> player_sleep_quality = df.where(player_mask).get_series<double>("Sleep_Quality");
            cout <<"    " << name <<":\n";
            cout << "           mean: " << player_sleep_quality.mean()<<"   ";
            cout <<"median: " << player_sleep_quality.median()<<"   ";
            cout <<"mode: " << player_sleep_quality.mode() <<"\n";
        }
        cout<<'\n';


    cout <<"Queries\n\n1. Which players have a higher maximum speed during Game sessions than Practice sessions?\n\n";

        for (const auto& [name, player_mask] : players_mask_map){
            double mean_practice = df.where(player_mask & practice_session_mask).get_series<double>("Max_Speed").mean();
            double mean_game     = df.where(player_mask & game_session_mask).get_series<double>("Max_Speed").mean();
            if (mean_game > mean_practice)
                cout << "    " << name << "  (Game: " << mean_game << "  >  Practice: " << mean_practice << ")\n";
        }
        cout<<'\n';

    cout << "2. Which players have the worst average sleep quality?\n\n";
        double worst = 1e9;
        for (const auto& [name, player_mask] : players_mask_map){
            double mean_sleep = df.where(player_mask).get_series<double>("Sleep_Quality").mean();
            if (mean_sleep < worst) worst = mean_sleep;
        }
        for (const auto& [name, player_mask] : players_mask_map){
            double mean_sleep = df.where(player_mask).get_series<double>("Sleep_Quality").mean();
            if (mean_sleep == worst)
                cout << "    " << name << " : " << mean_sleep << "\n";
        }
        cout<<'\n';

    return 0;
}
