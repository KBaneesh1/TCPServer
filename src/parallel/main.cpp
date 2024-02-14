#include <iostream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include<bits/stdc++.h>
#include<map>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include<sstream>
#include<typeinfo>
#define NUM_THREADS 10

using namespace std;

// Function prototypes
int initializeServer(int port);
int acceptConnection(int serverSd);
void handleClient(int *p_socket);
void closeConnection(int serverSd, int clientSd);

// data structure initialisation

pthread_mutex_t map_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t req_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_var = PTHREAD_COND_INITIALIZER;


map<string,string>mp;
queue<int>req; // put the request to queue , then handle it
pthread_t arr[NUM_THREADS]; //array of threads


void *scheduler(void *arg){
    while(true){
        try{
        // looping until request queue is not empty 
            // while(true){
            //     pthread_mutex_lock(&req_lock);
            //     if(req.empty()){
            //         pthread_mutex_unlock(&req_lock);
            //         continue;
            //     }
            //     else{
            //         break;
            //     }
            // }
            // // pthread_mutex_lock(&req_lock);
            // if(!req.empty()){
            //     int pclient= req.front(); 
            //     req.pop();
            //     pthread_mutex_unlock(&req_lock);

            //     cout<<"scheduler thread running on port: "<<pclient<<endl;
            //     handleClient(pclient);
            // }
            // else
            // {
            //     pthread_mutex_unlock(&req_lock);
                
            // }
            pthread_mutex_lock(&req_lock);
            int *pclient = NULL;
            if (req.empty())
            {
                pthread_cond_wait(&cond_var, &req_lock);
                // try again
                pclient = req.front();
                req.pop();
            }
            pthread_mutex_unlock(&req_lock);
            if (pclient != NULL)
            {
                // we have a connection
                handleClient(pclient);
            }

        }
        catch(const char *errormsg)
        {
            cerr<<"Error : "<<errormsg<<endl;
            break;
        }
    }
    cout<<"exiting scheduler thread"<<endl;
    pthread_exit(NULL);
    return NULL;
}


int initializeServer(int port)
{
    int serverSd = socket(AF_INET, SOCK_STREAM, 0);
    int isetoption = 1;
    setsockopt(serverSd,SOL_SOCKET,SO_REUSEADDR,(char*)&isetoption,sizeof(isetoption));
    if (serverSd < 0)
    {
        cerr << "Error establishing the server socket" << endl;
        exit(EXIT_FAILURE);
    }

    sockaddr_in servAddr;
    bzero((char *)&servAddr, sizeof(servAddr));
    servAddr.sin_family = AF_INET;
    servAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servAddr.sin_port = htons(port);

    if (bind(serverSd, (struct sockaddr *)&servAddr, sizeof(servAddr)) < 0)
    {
        cerr << "Error binding socket to local address" << endl;
        exit(EXIT_FAILURE);
    }

    cout << "Waiting for a client to connect..." << endl;

    if (listen(serverSd, 100) < 0)
    {
        cerr << "Error listening for connections" << endl;
        exit(EXIT_FAILURE);
    }

    return serverSd;
}

int acceptConnection(int serverSd)
{
    sockaddr_in newSockAddr;
    socklen_t newSockAddrSize = sizeof(newSockAddr);
    int clientSd = accept(serverSd, (sockaddr *)&newSockAddr, &newSockAddrSize);
    if (clientSd < 0)
    {
        cerr << "Error accepting request from client!" << endl;
        exit(EXIT_FAILURE);
    }
    cout << "Connected with client!" << endl;
    return clientSd;
}


void handleClient(int *p_socket)
{
    int clientSd = *p_socket;
    //cout<<"client socket="<<clientSd<<endl;
    cout<<"hi from handleClient"<<endl;
    // the empty position is taken by the thread
    // the req is being handled , so it is popped out
    
    int bytesRead = 0, bytesWritten = 0;
    char buffer[4096];
    cout << "Awaiting client response..." << endl;
    memset(&buffer, 0, sizeof(buffer));


    // Read data from the client
    if ((bytesRead = recv(clientSd, buffer, sizeof(buffer), 0)) <= 0)
        {
            cerr << "Error reading from client!" << endl;
           return ;
    }
    // cout<<buffer<<endl;
    while(true){

        string receivedData(buffer);
        stringstream ss(receivedData);
        string line;

        // Split the received data into lines 
        // Process each line

        while (getline(ss, line, '\n'))
        {
            cout<<line<<endl;
            if (line == "END")
            {
                cout << "Client has quit the session" << endl;
                close(clientSd);
                // adding back the used pos to empty queue
                return;
            }

            string store="NULL\n";
            if(line== "WRITE")
            {

                string key,value;
                getline(ss,key,'\n');
                getline(ss,value,'\n');
                pthread_mutex_lock(&map_lock);
                if(value[0]==':')
                    value.erase(0,1);
                mp[key] = value;
               	pthread_mutex_unlock(&map_lock);
                store = "FIN\n";
                
            }
            else
            if(line=="READ")
            {
                string key;
                getline(ss,key,'\n');
                if(mp.find(key)!=mp.end()){
                    store = mp[key]+"\n";
                }
                
            }
            else
            if (line== "DELETE")
            {
                string key;
                getline(ss,key,'\n');
                if(mp.find(key)!=mp.end()){
                    pthread_mutex_lock(&map_lock);
                    mp.erase(key);
                    pthread_mutex_unlock(&map_lock);
                    store = "FIN\n";
                }
            }
            else
            if(line=="COUNT")
            {
                store = to_string(mp.size())+"\n";
            }

            // Send the response to the client
            bytesWritten = send(clientSd, store.c_str(), store.size(), 0);
        }	
    }
    close(clientSd);
    
    // after all the operations are done add the used arr position to empty arr
    return;
}

void closeConnection(int serverSd, int clientSd)
{
    close(clientSd);
    cout << "Connection closed..." << endl;
}

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        cerr << "Usage: " << argv[0] << " <port>" << endl;
        exit(EXIT_FAILURE);
    }

    int port = atoi(argv[1]);
    int serverSd = initializeServer(port);
    
    // initializing empty queue with all the thread empty positions
    for(int i=0;i<NUM_THREADS;i++){
        pthread_create(&arr[i],NULL,&scheduler,NULL);
    }
    // pthread_t t;
    while (true)
    {
        int clientSd = acceptConnection(serverSd);

        // for pushing the request and checking empty
        pthread_mutex_lock(&req_lock);
        req.push(clientSd);
        pthread_cond_signal(&cond_var);
        pthread_mutex_unlock(&req_lock);

    }
    // for (int i = 0; i < NUM_THREADS; ++i) {
    //     pthread_join(&arr[i], NULL);
    // }
    // close the server after all the work done
    close(serverSd);
    printf("Server socket finished!!");

    return 0;
}


