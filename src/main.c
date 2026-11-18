#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "server.h"
#include "util.h"

static void usage(const char *p) {
    fprintf(stderr,
            "usage: %s --id N --nodes host:cport:rport,host:cport:rport,... --dir PATH [options]\n"
            "\n"
            "  --id N               this node's id (1-based position in --nodes)\n"
            "  --nodes LIST         every cluster member, in id order; cport = client (RESP) port,\n"
            "                       rport = Raft peer port\n"
            "  --dir PATH           data directory (WAL + snapshot)\n"
            "  --bind ADDR          listen address (default: this node's host from --nodes)\n"
            "  --election-min MS    election timeout lower bound   (default 150)\n"
            "  --election-max MS    election timeout upper bound   (default 300)\n"
            "  --heartbeat MS       leader heartbeat interval      (default 50)\n"
            "  --snapshot-every N   entries between snapshots, 0=off (default 50000)\n"
            "  --no-fsync           do not fsync the WAL (NOT crash safe; benchmarking only)\n"
            "  --no-prevote         disable PreVote\n"
            "  --no-check-quorum    disable leader CheckQuorum\n"
            "  --log LEVEL          quiet|warn|info|debug (default info)\n",
            p);
}

static int parse_nodes(server_cfg_t *c, char *list) {
    int n = 0;
    for (char *tok = strtok(list, ","); tok; tok = strtok(NULL, ",")) {
        if (n >= RAFT_MAX_NODES) return -1;
        char host[64];
        int cp, rp;
        char *a = strchr(tok, ':');
        if (!a) return -1;
        size_t hl = (size_t)(a - tok);
        if (hl == 0 || hl >= sizeof host) return -1;
        memcpy(host, tok, hl);
        host[hl] = 0;
        if (sscanf(a + 1, "%d:%d", &cp, &rp) != 2) return -1;
        snprintf(c->host[n], sizeof c->host[n], "%s", host);
        c->cport[n] = cp;
        c->rport[n] = rp;
        n++;
    }
    c->n = n;
    return n > 0 ? 0 : -1;
}

int main(int argc, char **argv) {
    server_cfg_t c;
    memset(&c, 0, sizeof c);
    raft_cfg_default(&c.raft);
    c.do_fsync = 1;
    c.snapshot_every = 50000;
    c.id = 0;
    char *nodes = NULL;
    for (int i = 1; i < argc; i++) {
        const char *a = argv[i];
        const char *v = i + 1 < argc ? argv[i + 1] : NULL;
        if (!strcmp(a, "--id") && v) { c.id = atoi(v); i++; }
        else if (!strcmp(a, "--nodes") && v) { nodes = strdup(v); i++; }
        else if (!strcmp(a, "--dir") && v) { snprintf(c.dir, sizeof c.dir, "%s", v); i++; }
        else if (!strcmp(a, "--bind") && v) { snprintf(c.bind, sizeof c.bind, "%s", v); i++; }
        else if (!strcmp(a, "--election-min") && v) { c.raft.election_min_ms = (uint32_t)atoi(v); i++; }
        else if (!strcmp(a, "--election-max") && v) { c.raft.election_max_ms = (uint32_t)atoi(v); i++; }
        else if (!strcmp(a, "--heartbeat") && v) { c.raft.heartbeat_ms = (uint32_t)atoi(v); i++; }
        else if (!strcmp(a, "--snapshot-every") && v) { c.snapshot_every = strtoull(v, NULL, 10); i++; }
        else if (!strcmp(a, "--no-fsync")) c.do_fsync = 0;
        else if (!strcmp(a, "--no-prevote")) c.raft.prevote = 0;
        else if (!strcmp(a, "--no-check-quorum")) c.raft.check_quorum = 0;
        else if (!strcmp(a, "--log") && v) {
            g_log_level = !strcmp(v, "quiet") ? LOG_QUIET : !strcmp(v, "warn") ? LOG_WARN : !strcmp(v, "debug") ? LOG_DEBUG : LOG_INFO;
            i++;
        } else { usage(argv[0]); return 2; }
    }
    if (!nodes || c.id < 1 || !c.dir[0] || parse_nodes(&c, nodes) != 0 || c.id > c.n) { usage(argv[0]); return 2; }
    if (c.raft.election_max_ms < c.raft.election_min_ms) c.raft.election_max_ms = c.raft.election_min_ms;
    if (c.raft.election_min_ms < 3 * c.raft.heartbeat_ms)
        fprintf(stderr, "warning: election-min should be at least 3x heartbeat to avoid spurious elections\n");
    return server_run(&c);
}
