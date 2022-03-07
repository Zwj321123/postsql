#include <iostream>
#include <pqxx/pqxx>
#include <fstream>
#include <string>


#include "exerciser.h"

using namespace std;
using namespace pqxx;

void exeSQL(string sql, connection *C){
  //create transaction objct
  work W(*C);
  //execute sql script
  W.exec(sql);
  W.commit();
}

void dropTable(connection *C, string tableName){
  string sql = "DROP TABLE IF EXISTS " + tableName + " CASCADE;";
  exeSQL(sql, C);
  cout<<"Drop table " + tableName+ " successfully"<<endl;
}

void dropAllTables(connection * C){
  
  dropTable(C, "PLAYER");
  dropTable(C, "TEAM");
  dropTable(C, "STATE");
  dropTable(C, "COLOR");
}

void createPlayer(connection *C){
  string sql;
  sql = "CREATE TABLE PLAYER(" \
    "PLAYER_ID SERIAL PRIMARY KEY," \
    "TEAM_ID INT," \
    "UNIFORM_NUM INT," \
    "FIRST_NAME VARCHAR(256)," \
    "LAST_NAME VARCHAR(256)," \
    "MPG INT," \
    "PPG INT," \
    "RPG INT," \
    "APG INT," \
    "SPG DOUBLE PRECISION," \
    "BPG DOUBLE PRECISION," \
    "FOREIGN KEY (TEAM_ID) REFERENCES TEAM(TEAM_ID) ON DELETE SET NULL ON UPDATE CASCADE);;";
  exeSQL(sql, C);
  cout<<"Table Player created successfully"<<endl;
}


void createTeam(connection *C){
  string sql;
  sql = "CREATE TABLE TEAM(" \
    "TEAM_ID SERIAL PRIMARY KEY," \
    "NAME VARCHAR(256)," \
    "STATE_ID INT," \
    "COLOR_ID INT," \
    "WINS INT," \
    "LOSSES INT," \
    "FOREIGN KEY (STATE_ID) REFERENCES STATE(STATE_ID) ON DELETE SET NULL ON UPDATE CASCADE," \
    "FOREIGN KEY (COLOR_ID) REFERENCES COLOR(COLOR_ID) ON DELETE SET NULL ON UPDATE CASCADE);";
  exeSQL(sql, C);
  cout<<"Table Team created successfully"<<endl;
}

void createColor(connection * C){
  string sql;
  sql = "CREATE TABLE COLOR(" \
    "COLOR_ID SERIAL PRIMARY KEY," \
    "NAME VARCHAR(256));";
  exeSQL(sql, C);
  cout<<"Table Color created successfully"<<endl;
}

void createState(connection * C){
  string sql;
  sql = "CREATE TABLE STATE(" \
    "STATE_ID SERIAL PRIMARY KEY," \
    "NAME VARCHAR(256));";
  exeSQL(sql, C);
  cout<<"Table State created successfully"<<endl;
}

void createAllTables(connection * C){
  createState(C);
  createColor(C);
  createTeam(C);
  createPlayer(C);
}

void insertColor(string fname, connection *C){
  string name;
  string line;
  int color_id;
  ifstream colorFile(fname.c_str());
  if (colorFile.is_open()){
    while(getline(colorFile, line)){
      stringstream ss;
      ss<<line;
      ss >> color_id >> name;
      add_color(C, name);
    }
    colorFile.close();
  }
  else{
    cout<< "Unable to open " + fname << endl;
    return;
  }
  return;
}

void insertState(string fname, connection *C){
  string name;
  string line;
  int state_id;
  ifstream stateFile(fname.c_str());
  if (stateFile.is_open()){
    while(getline(stateFile, line)){
      stringstream ss;
      ss<<line;
      ss >> state_id >> name;
      add_state(C, name);
    }
    stateFile.close();
  }
  else{
    cout<< "Unable to open " + fname <<endl;
    return;
  }
  return;
}

void insertPlayer(string fname, connection *C){
  string line, first_name, last_name;
  int player_id, team_id, jersey_num, mpg, ppg, rpg, apg;
  double spg, bpg;
  ifstream playerFile(fname.c_str());
  if (playerFile.is_open()){
    while(getline(playerFile, line)){
      stringstream ss;
      ss<<line;
      ss>>player_id >> team_id >> jersey_num >> first_name >> last_name >> mpg >> ppg >> rpg >> apg >> spg >> bpg;
      add_player(C, team_id, jersey_num, first_name, last_name, mpg, ppg, rpg, apg, spg, bpg);      
    }
    playerFile.close();
  }
  else{
    cout<< "Unable to open "+ fname <<endl;
    return;
  }
  return;
}

void insertTeam(string fname, connection *C){
  string line, name;
  int team_id, state_id, color_id, wins, losses;
  ifstream teamFile(fname.c_str());
  if (teamFile.is_open()){
    while(getline(teamFile, line)){
      stringstream ss;
      ss << line;
      ss >> team_id >> name >> state_id >> color_id >> wins >> losses;
      add_team(C, name, state_id, color_id, wins, losses);
    }
    teamFile.close();
  }
  else{
    cout << "Unable to open "+ fname<<endl;
    return;
  }
  return;
}

int main (int argc, char *argv[]) 
{

  //Allocate & initialize a Postgres connection object
  connection *C;

  try{
    //Establish a connection to the database
    //Parameters: database name, user name, user password
    C = new connection("dbname=ACC_BBALL user=postgres password=passw0rd");
    if (C->is_open()) {
      cout << "Opened database successfully: " << C->dbname() << endl;
    } else {
      cout << "Can't open database" << endl;
      return 1;
    }
  } catch (const std::exception &e){
    cerr << e.what() << std::endl;
    return 1;
  }


  //TODO: create PLAYER, TEAM, STATE, and COLOR tables in the ACC_BBALL database
  //      load each table with rows from the provided source txt files
  //delete if exist
  dropAllTables(C);
  //create table
  createAllTables(C);
  //insert tables
  insertState("state.txt", C);
  insertColor("color.txt", C);
  insertTeam("team.txt", C);
  insertPlayer("player.txt", C);
  
  exercise(C);


  //Close database connection
  C->disconnect();

  return 0;
}


