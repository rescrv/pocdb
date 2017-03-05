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

// e
#include <e/serialization.h>

// BusyBee
#include <busybee.h>

// pocdb
#include <pocdb.h>
#include "common.h"

struct pocdb_client
{
    pocdb_client();

    unsigned reqno;
    controller control;
    const std::auto_ptr<busybee_client> busybee;
};

pocdb_client :: pocdb_client()
    : reqno(0)
    , control()
    , busybee(busybee_client::create(&control))
{
}

pocdb_client* pocdb_create() { return new pocdb_client(); }
void pocdb_destroy(pocdb_client* client) { delete client; }

pocdb_returncode
pocdb_put(pocdb_client* client,
          const char* key, size_t key_sz,
          const char* val, size_t val_sz)
{
    const uint64_t host = HOSTS[client->reqno++ % NUM_HOSTS];
    const e::slice k(key, key_sz);
    const e::slice v(val, val_sz);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + 1
                    + pack_size(k)
                    + pack_size(v);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << uint8_t('P') << k << v;
    if (client->busybee->send(host, msg) != BUSYBEE_SUCCESS) return POCDB_SERVER_ERROR;

    uint64_t resp;
    if (client->busybee->recv(-1, &resp, &msg) != BUSYBEE_SUCCESS) return POCDB_SERVER_ERROR;
    pocdb_returncode rc;
    e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
    up = up >> e::unpack_uint8<pocdb_returncode>(rc);
    return !up.error() ? rc : POCDB_SERVER_ERROR;
}

pocdb_returncode
pocdb_get(pocdb_client* client,
          const char* key, size_t key_sz,
          char** val, size_t* val_sz)
{
    const uint64_t host = HOSTS[client->reqno++ % NUM_HOSTS];
    const e::slice k(key, key_sz);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + 1
                    + pack_size(k);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << uint8_t('G') << k;
    if (client->busybee->send(host, msg) != BUSYBEE_SUCCESS) return POCDB_SERVER_ERROR;

    uint64_t resp;
    if (client->busybee->recv(-1, &resp, &msg) != BUSYBEE_SUCCESS) return POCDB_SERVER_ERROR;
    pocdb_returncode rc;
    e::slice v;
    e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
    up = up >> e::unpack_uint8<pocdb_returncode>(rc) >> v;
    if (up.error()) return POCDB_SERVER_ERROR;
    if (rc != POCDB_SUCCESS) return rc;
    *val_sz = v.size();
    *val = v.size() ? (char*)malloc(v.size()) : (char*)NULL;
    if (!*val && v.size()) return POCDB_SEE_ERRNO;
    return !up.error() ? rc : POCDB_SERVER_ERROR;
}
