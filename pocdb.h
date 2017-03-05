/* Copyright (c) 2017, Robert Escriva, Cornell University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of pocdb nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef pocdb_h_
#define pocdb_h_

/* C */
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

/* pocdb_returncode occupies [6656, 6912) */
enum pocdb_returncode
{
    POCDB_SUCCESS,
    POCDB_NOT_FOUND,
    POCDB_SEE_ERRNO,
    POCDB_SERVER_ERROR,
    POCDB_INTERNAL,
    POCDB_GARBAGE
};

struct pocdb_client;

struct pocdb_client* pocdb_create();
void pocdb_destroy(struct pocdb_client* client);

enum pocdb_returncode pocdb_put(struct pocdb_client* client,
                                const char* key, size_t key_sz,
                                const char* val, size_t val_sz);
// get is not consistent---done to save time (no, you cannot just read from one
// replica to get a consistent read)
enum pocdb_returncode pocdb_get(struct pocdb_client* client,
                                const char* key, size_t key_sz,
                                char** val, size_t* val_sz);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* pocdb_h_ */
