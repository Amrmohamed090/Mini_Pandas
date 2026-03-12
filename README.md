# Soccer Performance Data Analysis

## Overview

This implementation is inspired by **Pandas**, the most commonly library for processing and analyzing tabular data (but it is python of course XD). The goal was to replicate a small subset of Pandas' functionality from scratch in C++: loading a CSV, storing it in a structured format, and enabling expressive queries over it.

The dataset contains soccer player performance records with fields like `Name`, `Session_Type`, `Max_Speed`, and `Sleep_Quality`. The analysis answers statistical questions about player performance across practice and game sessions.

---

## How It Works

### 1. Automatic Type Detection

Rather than storing all column types as strings, the read_csv() function detects them automatically using `csv::CSVStat`. It does a full scan of the file and records the type of every cell it encounters. From that, each column data type is assigned using this priority:

- If **any** cell is a string → the whole column is `string`
- Else if **any** cell is a double → the whole column is `double`
- Otherwise → the column is `int`

I am currently supporting only 3 data types, that is few of course, but it will do for now, and the code can be modified easily to support more.

Detecting the datatypes happens in **Pass 1**. Pass 2 then loads the actual data row by row.

---

### 2. Class Structure

#### `Series<T>`

A typed column. The template parameter `T` is the column's type (`int`, `double`, or `string`). Internally it's just a `std::vector<T>`, but it support statistical operations directly:

| Method | Description |
|---|---|
| `push_back(val)` | Append a value to the column |
| `operator[](i)` | Access value at index `i` |
| `get_data()` | Return a reference to the underlying vector |
| `mean()` | Arithmetic mean of all values |
| `median()` | Median (sorts a copy, picks middle) |
| `mode()` | Most frequent value |

Because the type is fixed at compile time, none of these methods need any branching, they work directly on `vector<T>`.

#### `DataFrame`

Stores all columns in a `unordered_map<string, AnySeries>` where `AnySeries = variant<Series<int>, Series<double>, Series<string>>`. This is the columnar format, data is stored column by column, not row by row. This makes column-level operations (mean, filter) fast because all values for a column sit together in memory.

| Method | Description |
|---|---|
| `read_csv(path)` | Load a CSV file. Pass 1 detects types, Pass 2 fills the columns |
| `get_series<T>(name)` | Get a typed reference to a column by name |
| `filter(column, value)` | Return a `Mask*` where rows matching `value` are set to 1 |
| `where(mask)` | Return a new DataFrame containing only the rows where the mask is 1 |
| `append(row)` | Add a new row (used internally by `where`) |
| `get_row(i)` | Get row `i` as a `vector<CellValue>` (used internally by `where`) |
| `value_counts<T>(col)` | Return a sorted `map<T, int>` of unique values and their frequency |
| `print_column_info()` | Print column names and their detected types |

---

### 3. Binary Mask Filtering

The querying approach uses **binary masks**, a bitset where each bit corresponds to a row. A `1` means the row passes the filter, `0` means it doesn't.

The advantage is that masks can be combined using standard bitwise operators (`&`, `|`, `~`), which makes multi-condition queries composable without creating intermediate DataFrames.

**Example — without masks**, filtering for "Practice sessions by Aaron" would require:
```cpp
DataFrame practice = df.where(df.filter("Session_Type", "Practice"));
DataFrame aaron_practice = practice.where(practice.filter("Name", "Aaron"));
```
Two full DataFrame copies are created.

**With masks**, you combine the conditions first, then materialize once:
```cpp
Mask practice_mask = df.filter("Session_Type", "Practice");
Mask aaron_mask    = df.filter("Name", "Aaron");

DataFrame result = df.where(practice_mask & aaron_mask);
```
One combined mask, one DataFrame copy. More importantly, this can easily be scaled adding a third condition is just another `&`.

In `main()`, player masks are pre-computed once and reused across all queries:
```cpp
Mask practice_session_mask = df.filter("Session_Type", "Practice");
Mask game_session_mask     = df.filter("Session_Type", "Game");

map<string, Mask> players_mask_map;
for (const auto& [name, _] : mp)
    players_mask_map[name] = df.filter("Name", name);

// Query: max speed per player in practice
for (const auto& [name, player_mask] : players_mask_map) {
    double mean = df.where(practice_session_mask & player_mask)
                    .get_series<double>("Max_Speed").mean();
    cout << name << " : " << mean << "\n";
}
```

#### Bitset and Its Limitation

The mask type is `std::bitset<MAX_ROWS>`. Bitset operations (`&`, `|`, `~`) are extremely fast — they operate on 64 bits at a time using CPU word instructions. The tradeoff is that `std::bitset` requires its size to be a **compile-time constant**, so `MAX_ROWS` must be set before compilation.

```cpp
const size_t MAX_ROWS = 1000;
using Mask = std::bitset<MAX_ROWS>;
```

For a future improvement, this could be replaced with `boost::dynamic_bitset`, which works the same way but accepts a runtime size — making the DataFrame work for any number of rows without recompiling.

---

## Build

```bash
make
./main.exe
```

Requires g++ with C++17 support.

---

## Example Usage

```cpp
DataFrame df;
df.read_csv("./data.csv");

// Pre-compute masks
Mask practice = df.filter("Session_Type", "Practice");
Mask game     = df.filter("Session_Type", "Game");
Mask aaron    = df.filter("Name", "Aaron");

// Mean max speed for Aaron in practice sessions
double mean = df.where(practice & aaron)
                .get_series<double>("Max_Speed").mean();

// Sleep quality stats for Aaron across all sessions
Series<double> sleep = df.where(aaron).get_series<double>("Sleep_Quality");
cout << sleep.mean() << " " << sleep.median() << " " << sleep.mode() << "\n";

// All unique players and how many records each has
map<string, int> players = df.value_counts<string>("Name");
```

---

## Project Structure

```
.
├── main.cpp                          # DataFrame implementation + analysis
├── csv.hpp                           # Header-only CSV parser (Vincent La)
├── Makefile
├── Soccer Performance Data/
│   └── Soccer Performance Data.csv
├── testing/
│   ├── analysis.py                   # Pandas reference implementation
│   └── requirements.txt
└── deprecated/
    └── final.cpp                     # Previous iteration
```
