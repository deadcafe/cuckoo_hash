
CURDIR:=$(PWD)


CFLAGS  = -g -O3 -mavx2 -mbmi -msse4.2 -Werror -Wextra -Wall -Wstrict-aliasing -std=gnu11 -pipe
CPPFLAGS = -c -I$(CURDIR) -D_GNU_SOURCE
LIBS =
LDFLAGS =

#CFLAGS += -funroll-loops -frerun-loop-opt
#CFLAGS += -fforce-addr

ifdef ENABLE_TRACER
CPPFLAGS += -DENABLE_TRACER
endif

ifdef TARGET_NB
CPPFLAGS += -DTARGET_NB=$(TARGET_NB)
endif

ifdef DISABLE_AVX2_DRIVER
CPPFLAGS += -DDISABLE_AVX2_DRIVER
endif

SRCS    =       \
	dc_hash_tbl.c \
	unit_test.c

OBJS = ${SRCS:.c=.o}
DEPENDS = .depend
TARGET = hash

.SUFFIXES:	.o .c
.PHONY:	all clean depend
all:	depend $(TARGET)
.c.o:
	$(CC) $(CFLAGS) $(CPPFLAGS) $<

$(TARGET):	$(OBJS)
	$(CC) -o $@ $^ $(LIBS) $(LDFLAGS)

$(OBJS):	Makefile

clean:
	rm -f $(OBJS) $(TARGET) $(DEPENDS) *~ core core.*

depend:	$(SRCS) Makefile
	-@ $(CC) $(CPPFLAGS) -MM -MG $(SRCS) > $(DEPENDS)

-include $(DEPENDS)
