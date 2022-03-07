#include "query_funcs.h"

#include <iomanip>
#include <iostream>
#include <pqxx/pqxx>
#include <sstream>
#include <string>
#include <vector>
using namespace std;

void add_player(connection *C, int team_id, int jersey_num, string first_name, string last_name,
                int mpg, int ppg, int rpg, int apg, double spg, double bpg)
{
  work W(*C);
  stringstream sql_ss;
  sql_ss<< "INSERT INTO PLAYER (TEAM_ID, UNIFORM_NUM, FIRST_NAME, LAST_NAME, MPG, PPG, RPG, APG, SPG, BPG) VALUES ("
     << team_id << ", "<< jersey_num << ", " << W.quote(first_name) << ", " << W.quote(last_name) << ", " << mpg << ", " << ppg << ", " << rpg << ", " << apg << ", " << spg << ", " << bpg << ");";
  string sql = sql_ss.str();
  W.exec(sql);
  W.commit();
  //cout<<"insert player successfully"<<endl;
}


void add_team(connection *C, string name, int state_id, int color_id, int wins, int losses)
{
  work W(*C);
  stringstream sql_ss;
  sql_ss<<"INSERT INTO TEAM (NAME, STATE_ID, COLOR_ID, WINS, LOSSES) VALUES ("
	<< W.quote(name) << ", " <<state_id << ", " << color_id << ", " << wins << ", " << losses << ");";
  string sql = sql_ss.str();
  W.exec(sql);
  W.commit();
  //cout << "insert team successfully"<<endl;
}


void add_state(connection *C, string name)
{
  work W(*C);
  string sql = "INSERT INTO STATE (NAME) VALUES (" + W.quote(name) + ");";
  W.exec(sql);
  W.commit();
  //cout<<"Insert state successfully"<<endl;
}


void add_color(connection *C, string name)
{
  work W(*C);
  string sql = "INSERT INTO COLOR (NAME) VALUES (" + W.quote(name) + ");";
  W.exec(sql);
  W.commit();
  //cout<<"Insert color successfully"<<endl;
}


void query1(connection *C,
	    int use_mpg, int min_mpg, int max_mpg,
            int use_ppg, int min_ppg, int max_ppg,
            int use_rpg, int min_rpg, int max_rpg,
            int use_apg, int min_apg, int max_apg,
            int use_spg, double min_spg, double max_spg,
            int use_bpg, double min_bpg, double max_bpg
            )
{
  stringstream sql;
  sql << "SELECT * FROM PLAYER ";
  int use_flag[6] = {use_mpg, use_ppg, use_rpg, use_apg, use_spg, use_bpg};
  vector<int> min_int_arr = {min_mpg, min_ppg, min_rpg, min_apg};
  vector<int> max_int_arr = {max_mpg, max_ppg, max_rpg, max_apg};
  vector<double> min_double_arr = {min_spg, min_bpg};
  vector<double> max_double_arr = {max_spg, max_bpg};
  vector<int> isUse = {use_mpg, use_ppg, use_rpg, use_apg, use_spg, use_bpg};
  vector<string> fieldNames = {"MPG", "PPG", "RPG", "APG", "SPG", "BPG"};

  bool isFirst = true;
  //min_int_arr, max_int_arr
  for(int i = 0; i < min_int_arr.size(); i++){
    if (isUse[i] != 0){
      if (isFirst){
	sql<<"WHERE ";
      }
      if (!isFirst){
	sql<<" AND ";
      }
      sql << fieldNames[i] << ">=" << min_int_arr[i] << " AND " << fieldNames[i]<<" <= "<<max_int_arr[i];
      isFirst = false;
    }
  }

  //min_double_arr, max_double_arr
  for(int j = 0; j < min_double_arr.size(); j++){
    if (isUse[j + 4] != 0){
      if (isFirst){
	sql << "WHERE ";
      }
      if (!isFirst){
	sql << " AND ";
      }
      sql << fieldNames[j+4] << " >= "<<min_double_arr[j] << " AND " << fieldNames[j+4] << " <= " << max_double_arr[j];
      isFirst = false;
    }
  }
  sql << ";";
  /* nontransaction object */
  nontransaction N(*C);
  /*implement nontransactional query*/
  result R(N.exec(sql.str()));
  cout << "PLAYER_ID TEAM_ID UNIFORM_NUM FIRST_NAME LAST_NAME MPG PPG RPG APG SPG BPG"<<endl;

  //print all queries
  for(auto c = R.begin(); c != R.end(); ++c){
    cout<<c[0].as<int>() << " " <<c[1].as<int>() << " " << c[2].as<int>() << " " <<
      c[3].as<string>() << " " << c[4].as<string>() << c[5].as<int>() << " "<<
      c[6].as<int>() << " " << c[7].as<int>() << " " << c[8].as<int>() << " " <<
      fixed << setprecision(1) << c[9].as<double>() << " " << c[10].as<double>()<<endl;
  }
  
}


void query2(connection *C, string team_color)
{
  nontransaction N(*C);
  stringstream sql;
  sql << "SELECT TEAM.NAME FROM TEAM, COLOR WHERE TEAM.COLOR_ID = "\
    "COLOR.COLOR_ID AND COLOR.NAME = "
      <<N.quote(team_color) << ";";

  result R(N.exec(sql.str()));
  N.commit();
  cout<<"NAME"<<endl;
  for(auto it = R.begin(); it != R.end(); ++it){
    cout<<it[0].as<string>()<<endl;
  }
  
}


void query3(connection *C, string team_name)
{
  nontransaction N(*C);
  stringstream sql;
  sql << "SELECT FIRST_NAME, LAST_NAME FROM PLAYER, TEAM WHERE PLAYER.TEAM_ID = "\
    "TEAM.TEAM_ID AND TEAM.NAME = "
      <<N.quote(team_name) << " ORDER BY PPG DESC;";

  result R(N.exec(sql.str()));
  N.commit();
  cout<<"FIRST_NAME LAST_NAME"<<endl;
  for(auto it = R.begin(); it != R.end(); ++it){
    cout<<it[0].as<string>()<<" "<<it[1].as<string>()<<endl;
  }  
}

/**
 * query4: show first name, last name, and jersey number of each player that plays in the 
 * indicated state and wears the indicated uniform color
 */
void query4(connection *C, string team_state, string team_color)
{
  nontransaction N(*C);
  stringstream sql;
  sql << "SELECT FIRST_NAME, LAST_NAME, UNIFORM_NUM FROM PLAYER, STATE, COLOR, TEAM WHERE PLAYER.TEAM_ID = "\
    "TEAM.TEAM_ID AND TEAM.COLOR_ID = COLOR.COLOR_ID AND TEAM.STATE_ID = STATE.STATE_ID AND STATE.NAME = "\
      <<N.quote(team_state)<<" AND " << "COLOR.NAME = "<<N.quote(team_color) << ";";

  result R(N.exec(sql.str()));
  N.commit();
  cout<<"FIRST_NAME LAST_NAME UNIFORM_NUM"<<endl;
  for(auto it = R.begin(); it != R.end(); ++it){
    cout<<it[0].as<string>()<<" "<<it[1].as<string>() <<" "<< it[2].as<int>()<<endl;
  }
}

/**
 * query5: show first name and last name of each player, and team name and number of
wins for each team that has won more than the indicated number of games
 */
void query5(connection *C, int num_wins)
{
  nontransaction N(*C);
  stringstream sql;
  sql << "SELECT FIRST_NAME, LAST_NAME, NAME, WINS FROM PLAYER, TEAM WHERE "\
    "PLAYER.TEAM_ID = TEAM.TEAM_ID AND WINS > "<<num_wins<<";";
  result R(N.exec(sql.str()));
  N.commit();
  cout<<"FIRST_NAME LAST_NAME NAME WINS"<<endl;
  for(auto it = R.begin(); it != R.end(); ++it){
    cout<<it[0].as<string>()<<" "<<it[1].as<string>()<<" "<<it[2].as<string>()<<" "<<it[3].as<int>()<<endl;
  }
}
