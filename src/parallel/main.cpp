#include <iostream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include<map>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include<sstream>
#include<typeinfo>


using namespace std;

// Function prototypes
int initializeServer(int port);
int acceptConnection(int serverSd);
void *handleClient(void *p_socket);
void closeConnection(int serverSd, int clientSd);

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
map<string,string>mp;

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        cerr << "Usage: " << argv[0] << " <port>" << endl;
        exit(EXIT_FAILURE);
    }

    int port = atoi(argv[1]);
    int serverSd = initializeServer(port);

    
    while (true)
    {
        int clientSd = acceptConnection(serverSd);
        pthread_t t;
        int *pclient = (int *)malloc(sizeof(int));
        *pclient = clientSd;
        pthread_create(&t, NULL, &handleClient, pclient);
        // handleClient(clientSd);
        //closeConnection(serverSd, clientSd);
    }
     close(serverSd);
    printf("Server socket finished!!");

    return 0;
}

int initializeServer(int port)
{
    int serverSd = socket(AF_INET, SOCK_STREAM, 0);
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


void *handleClient(void *p_socket)
{
    int clientSd = *((int *)p_socket);
    cout<<"client socket="<<clientSd<<endl;
    
    int bytesRead = 0, bytesWritten = 0;
    char buffer[4096];
    cout << "Awaiting client response..." << endl;
    memset(&buffer, 0, sizeof(buffer));

        // Read data from the client
    if ((bytesRead = recv(clientSd, buffer, sizeof(buffer), 0)) <= 0)
        {
            cerr << "Error reading from client!" << endl;
           return NULL;
        }
    cout<<buffer<<endl;
    while(true){
        // Split the received data into lines
        string receivedData(buffer);
        stringstream ss(receivedData);
        string line;

        while (getline(ss, line, '\n'))
        {
            //cout<<ss<<endl;
            cout<<line<<endl;
            if (line == "END")
            {
                cout << "Client has quit the session" << endl;
                close(clientSd);
        	pthread_exit(NULL);
                return NULL;
            }

            // Process each line
            string store="NULL\n";
            if(line== "WRITE")
            {

                string key,value;
                getline(ss,key,'\n');
                getline(ss,value,'\n');
                pthread_mutex_lock(&lock);
                if(value[0]==':')
                    value.erase(0,1);
                mp[key] = value;
                pthread_mutex_unlock(&lock);
                store = "FIN\n";
                // break;
            }
            else
            if(line=="READ")
            {
                string key;
                getline(ss,key,'\n');
                if(mp.find(key)!=mp.end()){
                    store = mp[key]+"\n";
                }
                // break;
            }
            else
            if (line== "DELETE")
            {
                string key;
                getline(ss,key,'\n');
                if(mp.find(key)!=mp.end()){
                    pthread_mutex_lock(&lock);
                    mp.erase(key);
                    pthread_mutex_unlock(&lock);
                    store = "FIN\n";
                }
            }
            else
            if(line=="COUNT")
            {
                store = to_string(mp.size())+"\n";
            }
            
            
            // Get the server's response
            
            // Send the response to the client
            bytesWritten = send(clientSd, store.c_str(), store.size(), 0);
        }	
      }
        close(clientSd);
        return NULL;

    // cout << " Bytes read: " << bytesRead << endl;
}

void closeConnection(int serverSd, int clientSd)
{
    close(clientSd);
    cout << "Connection closed..." << endl;
}

