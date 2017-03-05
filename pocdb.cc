// Copyright (c) 2017, Robert Escriva
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of pocdb nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

// POSIX
#include <signal.h>

// Google Log
#include <glog/logging.h>
#include <glog/raw_logging.h>

// LevelDB
#include <leveldb/comparator.h>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>

// po6
#include <po6/threads/thread.h>
#include <po6/time.h>

// e
#include <e/daemon.h>
#include <e/compat.h>
#include <e/guard.h>
#include <e/state_hash_table.h>
#include <e/strescape.h>

// pocdb
#include <pocdb.h>
#include "common.h"

#define CHECK_UNPACK(UNPACKER) \
    do \
    { \
        if (UNPACKER.error()) \
        { \
            LOG(WARNING) << __FILE__ << ":" << __LINE__ << " received corrupt message"; \
            return; \
        } \
    } while (0)

struct pocdaemon;

struct ballot
{
    uint64_t number;
    uint64_t leader;

    bool operator < (const ballot& rhs) const
    { return number < rhs.number ||
             (number == rhs.number && leader < rhs.leader); }
    bool operator == (const ballot& rhs) const
    { return number == rhs.number && leader == rhs.leader; }
    bool operator != (const ballot& rhs) const
    { return !(*this == rhs); }
    bool operator > (const ballot& rhs) const
    { return rhs < *this; }
};

e::packer
operator << (e::packer pa, const ballot& rhs)
{
    return pa << rhs.number << rhs.leader;
}

e::unpacker
operator >> (e::unpacker up, ballot& rhs)
{
    return up >> rhs.number >> rhs.leader;
}

size_t
pack_size(const ballot&)
{
    return 2 * sizeof(uint64_t);
}

struct pvalue
{
    pvalue() : b(), v() {}

    ballot b;
    std::string v;
};

e::packer
operator << (e::packer pa, const pvalue& rhs)
{
    return pa << rhs.b << e::slice(rhs.v);
}

e::unpacker
operator >> (e::unpacker up, pvalue& rhs)
{
    e::slice v;
    up = up >> rhs.b >> v;
    rhs.v.assign(v.cdata(), v.size());
    return up;
}

size_t
pack_size(const pvalue& p)
{
    return pack_size(p.b) + pack_size(e::slice(p.v));
}

struct write_state_machine
{
    write_state_machine(const std::string& k);

    const std::string& state_key();
    bool finished();
    void write(uint64_t c, const e::slice& v, pocdaemon* d);
    void phase1b(uint64_t c, uint64_t ver, const ballot& b, const pvalue& v, pocdaemon* d);
    void phase2b(uint64_t c, uint64_t ver, const ballot& b, pocdaemon* d);
    void retry(pocdaemon* d);

    void work_state_machine(pocdaemon* d);

    const std::string key;
    po6::threads::mutex mtx;
    std::list<std::pair<uint64_t, std::string> > values;

    // paxos state
    bool executing_paxos;
    ballot leading;
    std::vector<uint64_t> promises;
    std::vector<uint64_t> accepted;
    pvalue max_accepted;
    uint64_t version;

    private:
        write_state_machine(const write_state_machine&);
        write_state_machine& operator = (const write_state_machine&);
};

struct pocdaemon
{
    pocdaemon(uint64_t host);
    int run();

    void process_put(uint64_t c, std::auto_ptr<e::buffer> msg, e::unpacker up);
    void process_get(uint64_t c, std::auto_ptr<e::buffer> msg, e::unpacker up);
    void process_phase1a(uint64_t c, std::auto_ptr<e::buffer> msg, e::unpacker up);
    void process_phase1b(uint64_t c, std::auto_ptr<e::buffer> msg, e::unpacker up);
    void process_phase2a(uint64_t c, std::auto_ptr<e::buffer> msg, e::unpacker up);
    void process_phase2b(uint64_t c, std::auto_ptr<e::buffer> msg, e::unpacker up);
    void process_learn(uint64_t c, std::auto_ptr<e::buffer> msg, e::unpacker up);
    void process_retry(uint64_t c, std::auto_ptr<e::buffer> msg, e::unpacker up);

    pocdb_returncode get_acceptor_state(const e::slice& k, uint64_t* ver,
                                        ballot* b, pvalue* v);
    pocdb_returncode save_acceptor_state(const e::slice& k, uint64_t ver,
                                         const ballot& b, const pvalue& v);

    uint64_t host;
    e::garbage_collector gc;
    controller control;
    const std::auto_ptr<busybee_server> busybee;
    leveldb::DB* db;
    typedef e::state_hash_table<std::string, write_state_machine> write_map_t;
    write_map_t writes;

    private:
        pocdaemon(const pocdaemon&);
        pocdaemon& operator = (const pocdaemon&);
};

uint32_t s_interrupts = 0;

static void
exit_on_signal(int /*signum*/)
{
    RAW_LOG(ERROR, "interrupted: exiting");
    e::atomic::increment_32_nobarrier(&s_interrupts, 1);
}

int
main(int argc, const char* argv[])
{
    assert(argc == 2);
    uint64_t host = 0;

    if (strcmp("A", argv[1]) == 0) host = HOSTA;
    if (strcmp("B", argv[1]) == 0) host = HOSTB;
    if (strcmp("C", argv[1]) == 0) host = HOSTC;
    if (strcmp("D", argv[1]) == 0) host = HOSTD;
    if (strcmp("E", argv[1]) == 0) host = HOSTE;
    assert(host > 0);

    pocdaemon d(host);
    return d.run();
}

pocdaemon :: pocdaemon(uint64_t h)
    : host(h)
    , gc()
    , control()
    , busybee(busybee_server::create(&control, host, control.lookup(host), &gc))
    , db(NULL)
    , writes(&gc)
{
}

int
pocdaemon :: run()
{
    if (!e::block_all_signals())
    {
        std::cerr << "could not block signals; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!e::install_signal_handler(SIGHUP, exit_on_signal) ||
        !e::install_signal_handler(SIGINT, exit_on_signal) ||
        !e::install_signal_handler(SIGTERM, exit_on_signal) ||
        !e::install_signal_handler(SIGQUIT, exit_on_signal))
    {
        PLOG(ERROR) << "could not install signal handlers";
        return EXIT_FAILURE;
    }

    leveldb::Options opts;
    opts.create_if_missing = true;
    opts.filter_policy = leveldb::NewBloomFilterPolicy(10);
    opts.max_open_files = std::max(sysconf(_SC_OPEN_MAX) >> 1, 1024L);
    leveldb::Status st = leveldb::DB::Open(opts, ".", &db);

    if (!st.ok())
    {
        LOG(ERROR) << "could not open leveldb: " << st.ToString();
        return EXIT_FAILURE;
    }

    e::garbage_collector::thread_state ts;
    gc.register_thread(&ts);

    while (s_interrupts == 0)
    {
        uint64_t id;
        std::auto_ptr<e::buffer> msg;
        busybee_returncode rc = busybee->recv(&ts, -1, &id, &msg);

        if (rc != BUSYBEE_SUCCESS)
        {
            LOG(ERROR) << "busybee: " << rc;
            continue;
        }

        e::unpacker up(msg->unpack_from(BUSYBEE_HEADER_SIZE));
        uint8_t type;
        up = up >> type;

        if (up.error())
        {
            LOG(ERROR) << "bad message";
            continue;
        }

        switch (type)
        {
            case uint8_t('P'):
                process_put(id, msg, up);
                continue;
            case uint8_t('G'):
                process_get(id, msg, up);
                continue;
            case uint8_t('a'):
                process_phase1a(id, msg, up);
                continue;
            case uint8_t('b'):
                process_phase1b(id, msg, up);
                continue;
            case uint8_t('A'):
                process_phase2a(id, msg, up);
                continue;
            case uint8_t('B'):
                process_phase2b(id, msg, up);
                continue;
            case uint8_t('L'):
                process_learn(id, msg, up);
                continue;
            case uint8_t('R'):
                process_retry(id, msg, up);
                continue;
            default:
                LOG(ERROR) << "bad message";
                continue;
        }
    }

    return EXIT_SUCCESS;
}

void
pocdaemon :: process_get(uint64_t c, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    e::slice k;
    up = up >> k;
    CHECK_UNPACK(up);
    std::string key(k.cdata(), k.size());
    key.append("L");
    std::string val;
    leveldb::Status st = db->Get(leveldb::ReadOptions(), key, &val);
    pocdb_returncode rc = POCDB_SUCCESS;

    if (st.IsNotFound())
    {
        rc = POCDB_NOT_FOUND;
    }
    else if (st.ok())
    {
        rc = POCDB_SUCCESS;
    }
    else
    {
        rc = POCDB_SERVER_ERROR;
    }

    if (val.size() > 8) val.resize(val.size() - 8);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + 8 + pack_size(e::slice(val));
    msg.reset(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << e::pack_uint8<pocdb_returncode>(rc) << e::slice(val);
    busybee->send(c, msg);
}

void
pocdaemon :: process_put(uint64_t c, std::auto_ptr<e::buffer>, e::unpacker up)
{
    e::slice k;
    e::slice v;
    up = up >> k >> v;
    CHECK_UNPACK(up);

    write_map_t::state_reference sr;
    write_state_machine* sm = writes.get_or_create_state(k.str(), &sr);
    sm->write(c, v, this);
}

void
pocdaemon :: process_phase1a(uint64_t c, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    e::slice k;
    uint64_t ver;
    ballot b;
    up = up >> k >> ver >> b;
    CHECK_UNPACK(up);

    uint64_t cur_ver;
    ballot cur_b;
    pvalue cur_v;

    if (get_acceptor_state(k, &cur_ver, &cur_b, &cur_v) != POCDB_SUCCESS)
    {
        LOG(ERROR) << "could not get acceptor state";
        return;
    }

    if (c == b.leader && b > cur_b && ver >= cur_ver)
    {
        cur_ver = ver;
        cur_b = b;

        if (save_acceptor_state(k, cur_ver, cur_b, cur_v) != POCDB_SUCCESS)
        {
            LOG(ERROR) << "could not save acceptor state";
            return;
        }
    }

    const size_t sz = BUSYBEE_HEADER_SIZE
                    + 1 + sizeof(uint64_t)
                    + pack_size(k)
                    + pack_size(cur_b)
                    + pack_size(cur_v);
    msg.reset(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << uint8_t('b') << k << cur_ver << cur_b << cur_v;
    busybee->send(c, msg);
}

void
pocdaemon :: process_phase1b(uint64_t c, std::auto_ptr<e::buffer>, e::unpacker up)
{
    e::slice k;
    uint64_t ver;
    ballot b;
    pvalue v;
    up = up >> k >> ver >> b >> v;
    CHECK_UNPACK(up);

    write_map_t::state_reference sr;
    write_state_machine* sm = writes.get_or_create_state(k.str(), &sr);
    sm->phase1b(c, ver, b, v, this);
}

void
pocdaemon :: process_phase2a(uint64_t c, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    e::slice k;
    uint64_t ver;
    ballot b;
    pvalue v;
    up = up >> k >> ver >> b >> v;
    CHECK_UNPACK(up);

    uint64_t cur_ver;
    ballot cur_b;
    pvalue cur_v;

    if (get_acceptor_state(k, &cur_ver, &cur_b, &cur_v) != POCDB_SUCCESS)
    {
        LOG(ERROR) << "could not get acceptor state";
        return;
    }

    if (ver == cur_ver && b == cur_b)
    {
        if (save_acceptor_state(k, ver, b, v) != POCDB_SUCCESS)
        {
            LOG(ERROR) << "could not save acceptor state";
            return;
        }

        const size_t sz = BUSYBEE_HEADER_SIZE
                        + 1 + sizeof(uint64_t)
                        + pack_size(k)
                        + pack_size(cur_b);
        msg.reset(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE)
            << uint8_t('B') << k << cur_ver << cur_b;
        busybee->send(c, msg);
    }
    else
    {
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + 1
                        + pack_size(k);
        msg.reset(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE)
            << uint8_t('R') << k;
        busybee->send(c, msg);
        return;
    }
}

void
pocdaemon :: process_phase2b(uint64_t c, std::auto_ptr<e::buffer>, e::unpacker up)
{
    e::slice k;
    uint64_t ver;
    ballot b;
    up = up >> k >> ver >> b;
    CHECK_UNPACK(up);

    write_map_t::state_reference sr;
    write_state_machine* sm = writes.get_or_create_state(k.str(), &sr);
    sm->phase2b(c, ver, b, this);
}

void
pocdaemon :: process_learn(uint64_t, std::auto_ptr<e::buffer>, e::unpacker up)
{
    e::slice k;
    uint64_t ver;
    e::slice v;
    up = up >> k >> ver >> v;
    CHECK_UNPACK(up);
    // XXX there's a race condition here; should only write to leveldb if newly
    // learned value has (ver) higher than previously learned value

    std::string key(k.cdata(), k.size());
    std::string val(v.cdata(), v.size());
    key.append("L", 1);
    val.append((const char*)&ver, sizeof(ver));
    leveldb::WriteOptions opts;
    opts.sync = true;
    leveldb::Status st = db->Put(opts, key, val);

    if (!st.ok())
    {
        LOG(ERROR) << "leveldb error: " << st.ToString();
    }

    LOG(INFO) << "put \"" << e::strescape(k.str()) << "\" (" << ver << ") ->\"" << e::strescape(v.str()) << "\""; 
}

void
pocdaemon :: process_retry(uint64_t, std::auto_ptr<e::buffer>, e::unpacker up)
{
    e::slice k;
    CHECK_UNPACK(up);

    write_map_t::state_reference sr;
    write_state_machine* sm = writes.get_or_create_state(k.str(), &sr);
    sm->retry(this);
}

pocdb_returncode
pocdaemon :: get_acceptor_state(const e::slice& k, uint64_t* ver,
                                ballot* b, pvalue* v)
{
    *ver = 0;
    *b = ballot();
    *v = pvalue();

    std::string key(k.cdata(), k.size());
    key.append("A");
    std::string val;
    leveldb::Status st = db->Get(leveldb::ReadOptions(), key, &val);

    if (st.IsNotFound())
    {
        return POCDB_SUCCESS;
    }
    else if (!st.ok())
    {
        LOG(ERROR) << "error: " << st.ToString();
        return POCDB_SERVER_ERROR;
    }

    e::unpacker up(val);
    up = up >> *ver >> *b >> *v;

    if (up.error())
    {
        LOG(ERROR) << "corrupt acceptor";
        return POCDB_SERVER_ERROR;
    }

    key = std::string(k.cdata(), k.size());
    key.append("L");
    st = db->Get(leveldb::ReadOptions(), key, &val);
    uint64_t written = 0;

    if (st.ok())
    {
        written = *(uint64_t*)(val.data() + val.size() - 8);
    }

    if (*ver == written)
    {
        ++*ver;
        *b = ballot();
        *v = pvalue();
    }

    return POCDB_SUCCESS;
}

pocdb_returncode
pocdaemon :: save_acceptor_state(const e::slice& k, uint64_t ver,
                                 const ballot& b, const pvalue& v)
{
    std::string key;
    std::string val;
    key.assign(k.cdata(), k.size());
    key.append("A");
    e::packer(&val) << ver << b << v;
    leveldb::WriteOptions opts;
    opts.sync = true;
    leveldb::Status st = db->Put(opts, key, val);

    if (!st.ok())
    {
        LOG(ERROR) << "error: " << st.ToString();
        return POCDB_SERVER_ERROR;
    }

    return POCDB_SUCCESS;
}

write_state_machine :: write_state_machine(const std::string& k)
    : key(k)
    , mtx()
    , values()
    , executing_paxos()
    , leading()
    , promises()
    , accepted()
    , max_accepted()
    , version()
{
}

const std::string&
write_state_machine :: state_key()
{
    po6::threads::mutex::hold hold(&mtx);
    return key;
}

bool
write_state_machine :: finished()
{
    po6::threads::mutex::hold hold(&mtx);
    return values.empty();
}

void
write_state_machine :: write(uint64_t c, const e::slice& v, pocdaemon* d)
{
    po6::threads::mutex::hold hold(&mtx);
    values.push_back(std::make_pair(c, v.str()));
    work_state_machine(d);
}

void
write_state_machine :: phase1b(uint64_t c, uint64_t ver, const ballot& b, const pvalue& v, pocdaemon* d)
{
    po6::threads::mutex::hold hold(&mtx);

    if ((version != 0 && ver > version) || b > leading)
    {
        executing_paxos = false;
        version = ver;
        return work_state_machine(d);
    }

    version = ver;

    if (v.b != ballot() && v.b > max_accepted.b)
    {
        max_accepted = v;
    }

    promises.push_back(c);
    work_state_machine(d);
}

void
write_state_machine :: phase2b(uint64_t c, uint64_t ver, const ballot& b, pocdaemon* d)
{
    po6::threads::mutex::hold hold(&mtx);

    if (ver != version || b != leading)
    {
        return;
    }

    accepted.push_back(c);
    work_state_machine(d);
}

void
write_state_machine :: retry(pocdaemon* d)
{
    po6::threads::mutex::hold hold(&mtx);
    executing_paxos = false;
    ++version;
    return work_state_machine(d);
}

void
write_state_machine :: work_state_machine(pocdaemon* d)
{
    if (!executing_paxos && values.empty())
    {
        return;
    }
    else if (!executing_paxos)
    {
        executing_paxos = true;

        leading = ballot();
        leading.number = po6::wallclock_time();
        leading.leader = d->host;
        promises.clear();
        accepted.clear();
        max_accepted.b = ballot();
        max_accepted.v = values.begin()->second;
    }

    if (max_accepted.b > leading)
    {
        executing_paxos = false;
        return work_state_machine(d);
    }

    if (promises.size() < QUORUM)
    {
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + 1 + sizeof(uint64_t)
                        + pack_size(e::slice(key))
                        + pack_size(leading);

        for (size_t i = 0; i < NUM_HOSTS; ++i)
        {
            if (std::find(promises.begin(), promises.end(), HOSTS[i]) != promises.end())
            {
                continue;
            }

            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(BUSYBEE_HEADER_SIZE)
                << uint8_t('a') << e::slice(key) << version << leading;
            d->busybee->send(HOSTS[i], msg);
        }
    }
    else if (accepted.size() < QUORUM)
    {
        max_accepted.b = leading;
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + 1 + sizeof(uint64_t)
                        + pack_size(e::slice(key))
                        + pack_size(leading)
                        + pack_size(max_accepted);

        for (size_t i = 0; i < NUM_HOSTS; ++i)
        {
            if (std::find(accepted.begin(), accepted.end(), HOSTS[i]) != accepted.end())
            {
                continue;
            }

            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(BUSYBEE_HEADER_SIZE)
                << uint8_t('A') << e::slice(key) << version << leading << max_accepted;
            d->busybee->send(HOSTS[i], msg);
        }
    }
    else
    {
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + 1 + sizeof(uint64_t)
                        + pack_size(e::slice(key))
                        + pack_size(e::slice(max_accepted.v));

        for (size_t i = 0; i < NUM_HOSTS; ++i)
        {
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(BUSYBEE_HEADER_SIZE)
                << uint8_t('L') << e::slice(key) << version << e::slice(max_accepted.v);
            d->busybee->send(HOSTS[i], msg);
        }

        executing_paxos = false;
        ++version;

        if (max_accepted.v == values.front().second)
        {
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(BUSYBEE_HEADER_SIZE)
                << e::pack_uint8<pocdb_returncode>(POCDB_SUCCESS);
            d->busybee->send(values.begin()->first, msg);
            values.pop_front();
        }

        work_state_machine(d);
    }
}
