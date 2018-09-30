#include <iostream>
#include <map>
#include <cstdlib>
#include <string>
#include <vector>
#include <tuple>
#include <ctime>

using namespace std;

const int N_USERS = 2649429;
const int N_MOVIES = 17770;
const int FEATURES = 50;

vector< vector<long double> > p(N_USERS), q(N_MOVIES);
vector< tuple<int, int, long double> > X_train;
map<int, vector<int>> implicit;
vector<long double> bu(N_USERS), bm(N_MOVIES);
long double mv = 3.7;


void read_train(int limit) {
    FILE * fin = fopen("~/Documents/Programing/Technospere/datamining-2/netflix/train.txt", "r+");
    
    string s;
    int u, m;
    long double r;
    
    while (fscanf(fin, "%d,%d,%lf,%s", &m, &u, &r, &s) != EOF) {
        X_train.push_back(make_tuple(m - 1, u - 1, r));
        
        if (X_train.size() >= limit) {
            fclose(fin);
            break;
        }
    }
}

void read_implicit() {
    FILE * fin = fopen("/Users/dmitry103/Documents/Programing/Technospere/datamining-2/netflix/test.txt", "r+");
    
    string s;
    int u, m;
    long double r;
    
    while (fscanf(fin, "%d,%d,%s", &m, &u, &s) != EOF) {
        
        if (implicit.find(u) != implicit.end()) {
            implicit[u - 1].push_back(m - 1);
        } else {
            implicit[u - 1] = vector<int>();
            implicit[u - 1].push_back(m - 1);
        }
    }
    
    fclose(fin);
}

long double dot(int u, int m) {
    long double res = 0;
    
    for (int i = 0; i < p[0].size(); i++) {
        res += p[u][i] * q[m][i];
    }
    
    return res;
}

long double loss() {
    long double res = 0.0, pred;
    int u, m, r;
    
    for (int i = 0; i < X_train.size(); i++) {
        m = get<0>(X_train[i]);
        u = get<1>(X_train[i]);
        r = get<2>(X_train[i]);
        
        pred = mv + bu[u] + bm[m] + dot(u, m);
        res += (r - pred) * (r - pred) / X_train.size();
    }
    
    return res;
}

vector<long double> predict() {
    vector<long double> res;
    
    FILE * fin = fopen("~/Documents/Programing/Technospere/datamining-2/netflix/test.txt", "r+");
    
    string s;
    int u, m;
    
    while (fscanf(fin, "%d,%d,%s", &m, &u, &s) != EOF) {
        res.push_back(dot(u, m));
    }
    
    fclose(fin);
    
    return res;
}

void do_sgd(int n_epoch, long double lr, long double lmd) {
    vector< vector<long double> > p1(p.size()), q1(q.size());
    vector<int> cntu(p.size()), cntm(q.size()), bu1(bu.size()), bm1(bm.size());
    int m, u, i, j, k;
    long double r, mlt, dt;

    clock_t begin;
    
    for (i = 0; i < p1.size(); i++) {
        p1[i] = vector<long double>(p[0].size());
    }
    
    for (i = 0; i < q1.size(); i++) {
        q1[i] = vector<long double>(q[0].size());
    }
    
    for (i = 0; i < X_train.size(); i++) {
        m = get<0>(X_train[i]);
        u = get<1>(X_train[i]);
        cntu[u] += 1;
        cntm[m] += 1;
    }
    
    for (i = 0; i < n_epoch; i++) {
        begin = clock();
        
        if (i % 4 == 0) {
            for (j = 0; j < p.size(); j++) {
                for (k = 0; k < p[0].size(); k++) {
                    p1[j][k] = (1.0 - lmd * lr) * p[j][k];
                }
            }
        } else if (i % 4 == 1) {
            for (j = 0; j < q.size(); j++) {
                for (k = 0; k < q[0].size(); k++) {
                    q1[j][k] = (1.0 - lmd * lr) * q[j][k];
                }
            }
        } else if (i % 4 == 2) {
            bu1.assign(bu1.size(), 0);
        } else if (i % 4 == 3) {
            bm1.assign(bm1.size(), 0);
        }
        
        for (j = 0; j < X_train.size(); j++) {
            m = get<0>(X_train[j]);
            u = get<1>(X_train[j]);
            r = get<2>(X_train[j]);
            
            mlt = dot(u, m);
           
            if (i % 4 == 0) {
                // p_u stage
                dt = mlt + bu[u] + bm[m] + mv;
                
                for (k = 0; k < p[0].size(); k++) {
                    p1[u][k] += lr * (r - dt) * q[m][k] / cntu[u];
                }
            } else if (i % 4 == 1) {
                // q_m stage
                dt = mlt + bu[u] + bm[m] + mv;
                
                for (k = 0; k < q[0].size(); k++) {
                    q1[m][k] += lr * (r - dt) * p[u][k] / cntm[m];
                }
            } else if (i % 4 == 2) {
                // bu stage
                dt = mlt + bm[m] + mv;
                bu1[u] += (r - dt) / (lmd + cntu[u]);
            } else if (i % 4 == 3) {
                // bm stage
                dt = mlt + bu[u] + mv;
                bm1[m] += (r - dt) / (lmd + cntm[m]);
            }
        }
        
        if (i % 4 == 0) {
            for (j = 0; j < p.size(); j++) {
                for (k = 0; k < p[0].size(); k++) {
                    p[j][k] = p1[j][k];
                }
            }
        } else if (i % 4 == 1) {
            for (j = 0; j < q.size(); j++) {
                for (k = 0; k < q[0].size(); k++) {
                    q[j][k] = q1[j][k];
                }
            }
        } else if (i % 4 == 2) {
            bu.assign(bu1.begin(), bu1.end());
        } else if (i % 4 == 3) {
            bm.assign(bm1.begin(), bm1.end());
        }
        
        cout << "TIME: " << double(clock() - begin) / CLOCKS_PER_SEC << endl;
//        cout << "EPOCH: " << i << ' ' << "LOSS: " << loss() << endl << endl;
    }
}

int main(int argc, const char * argv[]) {
    read_train(100000000);
//    read_implicit();

    srand(0);
    
    generate(bu.begin(), bu.end(), []() {return (long double) rand() / RAND_MAX;});
    generate(bm.begin(), bm.end(), []() {return (long double) rand() / RAND_MAX;});
    
    for (int i = 0; i < p.size(); i++) {
        p[i] = vector<long double>(FEATURES);
        generate(p[i].begin(), p[i].end(), []() {return (long double) rand() / RAND_MAX;});
    }
    
    for (int i = 0; i < q.size(); i++) {
        q[i] = vector<long double>(FEATURES);
        generate(q[i].begin(), q[i].end(), []() {return (long double) rand() / RAND_MAX;});
    }
    
    cout << "INIT FINISHED\n";
    
    do_sgd(20, 0.1, 0.2);
    
    cout << loss() << endl;
    
//    for (int i = 0; i < p.size(); )
    
//    generate(p.begin(), p.end(), []() {return rand();});
    
//    cout << implicit[6].size() << endl;
}
