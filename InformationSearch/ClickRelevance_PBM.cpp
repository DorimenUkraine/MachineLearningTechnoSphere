#include <iostream>
#include <cstdio>
#include <fstream>
#include <cstdlib>
#include <vector>
#include <set>
#include <map>

using namespace std;

const int MAX_N = 400000000;
const int N_EPOCH = 5;
const int SESSIONS_NUM = 31061526;

vector< vector<double> > gamma(2);
//vector<double> gamma;
map< pair< pair<int, int>, int>, int> u2rank;
map< pair< pair<int, int>, int>, int> suq;
map< pair< pair<int, int>, int>, double> aluq0, aluq1;
map< pair< pair<int, int>, int>, double> **aluqs;
set< pair< pair<int, int>, int> > cur_clicked, allowed;
set<int> cur_queries;

pair< pair<int, int>, int> make_triple(int a, int b, int c) {
    return make_pair(make_pair(a, b), c);
}

int main(int argc, const char * argv[]) {
    
    int s, t, i, j, cnt = 0, q, r, curs = -1, curq = -1, u;
    char act;
    pair< pair<int, int>, int> p, ps;
    double gmm;
    char *tmp = new char[100];
    bool flag = 0;
    
    vector<int> buf = vector<int>(10);
    aluqs = new map< pair< pair<int, int>, int>, double>* [2];
    aluqs[0] = &aluq0;
    aluqs[1] = &aluq1;
    
    gamma[0] = {0.41, 0.16, 0.11, 0.08, 0.06, 0.05, 0.04, 0.04, 0.025, 0.025};
    gamma[1] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};;
//    gamma = {0.41, 0.16, 0.11, 0.08, 0.06, 0.05, 0.04, 0.04, 0.025, 0.025};
    
    
    // Make set of allowed url-query pairs
    FILE *smp = fopen("~/Documents/Programing/Technospere/info-search-2/ClickRelevance/sample.csv", "r");
    fscanf(smp, "%[^\n]", tmp);
    cnt = 0;
    while (fscanf(smp, "%d_%d,%d", &q, &i, &u) == 3) {
        cnt++;

        p = make_triple(u, q, i);
        allowed.insert(p);
    }
    fclose(smp);
    
    
    cerr << "Finished with allowed set\n";
    cerr << "Allowed size = " << allowed.size() << endl;
    
    // initialize dicts
    fstream fin("~/Documents/Programing/Technospere/info-search-2/ClickRelevance/click_log_filtered.txt");
    cnt = 0;
    while (cnt < MAX_N && (fin >> s >> t >> act)) {
        cnt++;
        
        if (cnt % 10000000 == 0) {
            cerr << cnt << endl;
        }
        
        if (curs != s) {
            curs = s;
//            cur_queries.clear();
        }
        
        if (act == 'Q') {
            fin >> q >> r;
            
            for (i = 0; i < 10; i++) {
                fin >> buf[i];
                
                p = make_triple(buf[i], q, r);
                
                if (allowed.find(p) == allowed.end()) {
                    continue;
                }
                
                if (suq.find(p) == suq.end()) {
                    suq[p] = 1;
                } else {
//                    if (cur_queries.find(q) == cur_queries.end()) {
//                        suq[p]++;
//                    }
                    suq[p]++;
                }
                
                if (aluqs[0]->find(p) == aluqs[0][0].end()) {
                    aluqs[0][0][p] = 0;
                    aluqs[1][0][p] = 0;
                }
                
                if (u2rank.find(p) == u2rank.end()) {
                    u2rank[p] = i;
                }
            }
            
//            cur_queries.insert(q);
        } else {
            fin >> q;
        }
    }
    fin.close();
    cerr << "Finnished with init main dicts\n";
    cerr << "S_uq size = " << suq.size() << endl;
    
//    p = make_pair(2, 1112);
//    cout << suq[p] << endl;
    
    for (j = 0; j < N_EPOCH; j++) {
        fstream fin("~/Documents/Programing/Technospere/info-search-2/ClickRelevance/click_log_filtered.txt");
        cerr << "Starting epoch " << j << endl;
        
        cnt = 0;
        while (cnt < MAX_N && (fin >> s >> t >> act)) {
            cnt++;
            
            if (cnt % 10000000 == 0) {
                cerr << cnt << endl;
            }
            
            if (s != curs) {
                cur_clicked.clear();
                curs = s;
            }
            
            if (act == 'Q') {
                fin >> q >> r;
                
                if (q != curq) {
                    cur_clicked.clear();
                    curq = q;
                }
                
                for (i = 0; i < 10; i++) {
                    fin >> buf[i];
                    
                    p = make_triple(buf[i], q, r);
                    
                    if (allowed.find(p) == allowed.end()) {
                        continue;
                    }
                    
                    if (suq.find(p) == suq.end()) {
                        continue;
                    }
                    
                    
//                    if (q == 203452 && buf[i] == 3729994) {
//                        cerr << aluqs[(j + 1) % 2][0][p] << ' ' << aluqs[j % 2][0][p] << ' ' << gmm << ' ' << suq[p] << endl;
//                    }
                    
                    gmm = gamma[j % 2][u2rank[p]];
                    aluqs[(j + 1) % 2][0][p] += (aluqs[j % 2][0][p] * (1.0 - gmm) / (1.0 - gmm * aluqs[j % 2][0][p])) / suq[p];
                    
//                    if (q == 203452 && buf[i] == 3729994) {
//                        cerr << aluqs[(j + 1) % 2][0][p] << ' ' << aluqs[j % 2][0][p] << ' ' << gmm << ' ' << suq[p] << endl;
//                    }
                    
                    gamma[(j + 1) % 2][u2rank[p]] += (aluqs[j % 2][0][p] * (1.0 - gmm) / (1.0 - gmm * aluqs[j % 2][0][p])) / SESSIONS_NUM;
                    
//                    if (aluqs[(j + 1) % 2][0][p] < 0) {
//                        cerr << "error" << endl;
//                        cerr << aluqs[j % 2][0][p] << ' ' << gmm << ' ' << suq[p] << endl;
//                    }
                }
            } else {
                fin >> u;
                
                p = make_triple(u, curq, r);
                gmm = gamma[j % 2][u2rank[p]];
                
                if (allowed.find(p) == allowed.end()) {
                    continue;
                }
                
                if (suq.find(p) == suq.end()) {
                    continue;
                }
                
                if (cur_clicked.find(p) != cur_clicked.end()) {
                    continue;
                }
                cur_clicked.insert(p);
                
//                if (curq == 203452 && u == 3729994) {
//                    cerr << aluqs[(j + 1) % 2][0][p] << ' ' << aluqs[j % 2][0][p] << ' ' << gmm << ' ' << suq[p] << endl;
//                }
                
//                if (aluqs[(j + 1) % 2][0][p] < 0) {
//                    cerr << "error" << endl;
//                }
                
                aluqs[(j + 1) % 2][0][p] += (1.0 / suq[p]) - ((aluqs[j % 2][0][p] * (1.0 - gmm) / (1.0 - gmm * aluqs[j % 2][0][p])) / suq[p]);
                gamma[(j + 1) % 2][u2rank[p]] += (1.0 / SESSIONS_NUM) - ((aluqs[j % 2][0][p] * (1.0 - gmm) / (1.0 - gmm * aluqs[j % 2][0][p])) / SESSIONS_NUM);
                
//                if (curq == 203452 && u == 3729994) {
//                    cerr << aluqs[(j + 1) % 2][0][p] << ' ' << aluqs[j % 2][0][p] << ' ' << gmm << ' ' << suq[p] << endl;
//                }
                
//                if (aluqs[(j + 1) % 2][0][p] < 0) {
//                    cerr << "error" << endl;
//                }
            }
        }

        aluqs[j % 2][0].clear();
        gamma[j % 2].assign(10, 0);
        
        fin.close();
    }
    
    ofstream fout("~/Documents/Programing/Technospere/info-search-2/ClickRelevance/dump.txt");
    
    for (map< pair<pair<int, int>, int>, double>::iterator it = aluqs[j % 2][0].begin(); it != aluqs[j % 2][0].end(); it++) {
        fout << it->first.first.first << ' ' << it->first.first.second << ' ' << it->first.second << ' ' << it->second << endl;
        
        if (it->second > 1.0 + 1e-10) {
            cerr << "ERROR: one of the values is greater than 1.0\n" << "Key = ";
            cerr << it->first.first.first << ' ' << it->first.first.second << ' ' << it->first.second << endl;
        }
    }
    
    cerr << "G:";
    for (i = 0; i < 10; i++) {
        cerr << ' ' << gamma[j % 2][i];
    }
    cerr << endl;
}
