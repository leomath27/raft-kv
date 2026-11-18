#ifndef SERVER_H
#define SERVER_H

#include "raft.h"

typedef struct {
    int id, n;
    char host[RAFT_MAX_NODES][64];
    int cport[RAFT_MAX_NODES]; /* client (RESP) port per node, index id-1 */
    int rport[RAFT_MAX_NODES]; /* raft peer port per node */
    char bind[64];             /* bind address override ("" => the node's own host) */
    char dir[256];
    raft_cfg_t raft;
    int do_fsync;
    uint64_t snapshot_every; /* applied entries between snapshots; 0 disables */
} server_cfg_t;

int server_run(const server_cfg_t *cfg);

#endif
