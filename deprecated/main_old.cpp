#include <iostream>
#include <unordered_map>
#include <algorithm>

#include "rapidcsv.h"



// rapidcsv, taken from https://github.com/d99kris/rapidcsv


/*
Your implementation must calculate the following values:

1. The mean maximum speed for each player during Practice sessions.

2. The mean maximum speed for each player Game sessions.

3. The mean, median, and mode of sleep quality for each player.


Your implementation must produce output for the following queries:

1. Which players have a higher maximum speed during Game sessions than Practice sessions?

2. Which players have the worst average sleep quality?

*/





// just for debugging puroposes
void print_hash_map(const std::unordered_map<std::string, std::vector<float>*>&mp){
    std::cout<<"DEBUG: printing a hash map \n\n";
    for (const auto& pair : mp){
        std::cout <<"   " <<"key: " << pair.first << " -> values: " <<pair.second;

        // for (float value: *(pair.second)){
        //     std::cout << value << ", ";
        // }
        std::cout<<"\n";
    }
    std::cout<<"\n";
}



// a function that returns the mean, mod, and median of the vector
void get_stats(std::vector<float>* vec){
    if (vec->size() == 0){
        std::cout<<"WARNING: get stats was called on an empty vector";
        return;
    }
    // to get the median, we first need to sort the vector
    std::sort(vec->begin(), vec->end());

    // storing the median
    int median;

    int n = vec->size();

    median = (n % 2 == 0) ?  
         ((*vec)[n / 2] + (*vec)[(n / 2) - 1]) / 2 
        : (*vec)[n / 2];


    float sum = 0;
    // map to store the frequency of each value inside the vector
    std::unordered_map<float, int> values_frequency;


    // the most frequent value
    float mod;
    // the frequency of this value
    int freq;

    // iterateing over the vector
    for (const float &v: *vec){
        sum += v;

        // getting the mod
        values_frequency[v]++;

        if (values_frequency[v] > freq){
            freq = values_frequency[v];
            mod = v;
        }

    }

    float mean = sum/n;

    std::cout << "mean: "<< mean << " median: " << median << " mod: " << mod << "\n";
    


}
int main(){
    
    rapidcsv::Document doc("./Soccer Performance Data/Soccer Performance Data.csv");


    //getting the needed columns to perform the queries    
    std::vector<std::string> name_col  = doc.GetColumn<std::string>("Name");
    std::vector<float> max_speed_col = doc.GetColumn<float>("Max_Speed");
    std::vector<std::string> session_type_col = doc.GetColumn<std::string>("Session_Type");
    std::vector<float> sleep_quality_col = doc.GetColumn<float>("Sleep_Quality");

    // Name [string], Sleep quality [float], keeping it float for now
    std::unordered_map<std::string, std::vector<float>*> max_speed_per_player;

    std::unordered_map<std::string, std::vector<float>*> sleep_quality_per_player;
    


    float max_speed_practice_sum , max_speed_game_sum;

    for (int i = 0; i < max_speed_col.size(); i++){
        if (session_type_col[i] == "Practice"){
            max_speed_practice_sum += max_speed_col[i];
        }
        else if (session_type_col[i] == "Game"){
            max_speed_game_sum += max_speed_col[i];
        }
        

        // if the name has already been seen, push the sleep quality to the vector
        if (sleep_quality_map.find(name_col[i]) != sleep_quality_map.end()){
            sleep_quality_map[name_col[i]]->push_back(sleep_quality_col[i]);
        }
        // otherwise, initialize a new vector and push the sleep quality to it
        else{
            sleep_quality_map[name_col[i]] = new std::vector<float>;
        }
    }

    print_hash_map(sleep_quality_map);

    get_stats(sleep_quality_map["Santiago"]);



    

    // calculate the sum of the max_speed_average
    std::cout<<"max speed average for practice sessions = "<<max_speed_practice_sum/max_speed_col.size()<<"\n";

    return 0;
}