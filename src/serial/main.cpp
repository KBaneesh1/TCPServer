#include <iostream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include<map>
#include <sys/socket.h>
#include <netinet/in.h>
#include<sstream>

using namespace std;

// Function prototypes
int initializeServer(int port);
int acceptConnection(int serverSd);
void handleClient(int clientSd);
void closeConnection(int serverSd, int clientSd);

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
        handleClient(clientSd);
        closeConnection(serverSd, clientSd);
    }

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

    if (listen(serverSd, 15) < 0)
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


void handleClient(int clientSd)
{
    char buffer[1500];
    int bytesRead = 0, bytesWritten = 0;

    while (true)
    {
        cout << "Awaiting client response..." << endl;
        memset(&buffer, 0, sizeof(buffer));

        // Read data from the client
        if ((bytesRead = recv(clientSd, buffer, sizeof(buffer), 0)) <= 0)
        {
            cerr << "Error reading from client!" << endl;
            break;
        }

        // Split the received data into lines
        string receivedData(buffer);
        stringstream ss(receivedData);
        string line;

        while (getline(ss, line, '\n'))
        {
            if (line == "END")
            {
                cout << "Client has quit the session" << endl;
                close(clientSd);
                return;
            }

            // Process each line
            
                string store="NULL\n";
                if(line== "WRITE"){
                    string key,value;
                    getline(ss,key,'\n');
                    getline(ss,value,'\n');
                    if(value[0]==':')
                        value.erase(0,1);
                    mp[key] = value;
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
                if (line== "DELETE"){
                    string key;
                    getline(ss,key,'\n');
                    if(mp.find(key)!=mp.end()){
                        mp.erase(key);
                        store = "FIN\n";
                    }
                }
                else
                if(line=="COUNT"){
                    store = to_string(mp.size())+"\n";
                }
                
            
            // Get the server's response
            
            // Send the response to the client
            bytesWritten = send(clientSd, store.c_str(), store.size(), 0);
        }
    }

    // cout << " Bytes read: " << bytesRead << endl;
}

void closeConnection(int serverSd, int clientSd)
{
    close(clientSd);
    cout << "Connection closed..." << endl;
}
