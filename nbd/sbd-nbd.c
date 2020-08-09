#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// -----------------
// nbd server struct
// init

// start

// stop

// exit

// -----------------
// io context
// aio callback

// -----------------
// worker threads
// process request

// produce reply 

// -----------------
// do map

// -----------------
// do unmap

// -----------------
// do list

// -----------------
// load module

// -----------------
// ioctl setup

// -----------------
// handle signal

// -----------------
// parse arg

static const char* help = 
"Usage: sbd-nbd [options] map <vdi>              Map an vdi to nbd device\n"
"                         unmap <device | vdi>   Unmap nbd device\n"
"               [options] list-mapped            List mapped nbd devices\n"
"Map options:\n"
"  --device <device path>   Specify nbd device path (/dev/nbd{num})\n"
"  --read-only              Map read-only\n"
"  --nbds_max <limit>       Override module param nbds_max\n"
"  --max_part <limit>       Overried module param max_part\n"
"\n"
"List options:\n"
"  --format plain | json\n";

static void usage(void) {
    printf("%s\n", help);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        usage();
        return 0;
    }
    return 0;
}