#include <yajl/yajl_tree.h>
#include <yajl/yajl_gen.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LEN 128

typedef struct N {
    int nid;
} N;

typedef struct S {
    int id;
    size_t buf_len;
    N n;
    char buf[MAX_LEN];
} S;

void printS(S* s) {
    printf("id: %d\n", s->id);
    printf("buf_len: %d\n", s->buf_len);
    printf("n.nid: %d\n", s->n.nid);
    printf("buf: %s\n", s->buf);
}

void S2J(S* s, unsigned char** j) {
    yajl_gen g;
    //yajl_gen_status r;
    const unsigned char* tmp;
    size_t len;

    g = yajl_gen_alloc(NULL);
    yajl_gen_map_open(g);

    yajl_gen_string(g, "id", sizeof("id"));
    yajl_gen_integer(g, s->id);

    yajl_gen_string(g, "buf_len", sizeof("buf_len"));
    yajl_gen_integer(g, s->buf_len);

    yajl_gen_string(g, "n", sizeof("n"));
    yajl_gen_map_open(g);
    yajl_gen_string(g, "nid", sizeof("nid"));
    yajl_gen_integer(g, s->n.nid);
    yajl_gen_map_close(g);

    yajl_gen_string(g, "buf", sizeof("buf"));
    yajl_gen_string(g, s->buf, s->buf_len);

    yajl_gen_map_close(g);

    yajl_gen_get_buf(g, &tmp, &len);

    *j = malloc((*len)*sizeof(char));
    memcpy(*j, tmp, *len);

    yajl_gen_clear(g);
    yajl_gen_free(g);
}

void J2S(const unsigned char* j, S* s) {
    yajl_val node;

    node = yajl_tree_parse(j, NULL, 0);

    const char* id_path[] = { "id", (const char*)0 };
    yajl_val id_v = yajl_tree_get(node, id_path, yajl_t_number);
    s->id = YAJL_GET_INTEGER(id_v);

    const char* len_path[] = { "buf_len", (const char*)0 };
    yajl_val len_v = yajl_tree_get(node, len_path, yajl_t_number);
    s->buf_len = YAJL_GET_INTEGER(len_v);

    const char* nid_path[] = { "n", "nid", (const char*)0 };
    yajl_val nid_v = yajl_tree_get(node, nid_path, yajl_t_number);
    s->n.nid = YAJL_GET_INTEGER(nid_v);

    const char* buf_path[] = { "buf", (const char*)0 };
    yajl_val buf_v = yajl_tree_get(node, buf_path, yajl_t_string);
    memcpy(s->buf, YAJL_GET_STRING(buf_v), s->buf_len);

    yajl_tree_free(node);
}

int main() {
    S s = { 123, 8, {333}, "1234567" };
    unsigned char* jbuf;

    S2J(&s, &jbuf);
    printf("json: %s\n", jbuf);

    S sc;
    J2S(jbuf, &sc);
    printS(&sc);

    free(jbuf);

    return 0;
}
