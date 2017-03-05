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

#ifndef pocdb_common_h_
#define pocdb_common_h_

// BusyBee
#include <busybee.h>

#define HOSTA (0xdeadbeefULL << 32)
#define HOSTB (0xbad1deafULL << 32)
#define HOSTC (0x1eaff00dULL << 32)
#define HOSTD (0xdefec8edULL << 32)
#define HOSTE (0xcafebabeULL << 32)
#define NUM_HOSTS 5
#define QUORUM (NUM_HOSTS / 2 + 1)

uint64_t HOSTS[] = { HOSTA, HOSTB, HOSTC, HOSTD, HOSTE };

class controller : public busybee_controller
{
    public:
        controller() {}
        ~controller() throw () {}

    public:
        virtual po6::net::location lookup(uint64_t server_id)
        {
            for (unsigned i = 0; i < NUM_HOSTS; ++i)
            {
                po6::net::location loc;

                if (HOSTS[i] == server_id && loc.set("127.0.0.1", 2000 + i))
                {
                    return loc;
                }
            }

            return po6::net::location();
        }
};

#endif // pocdb_common_h_
