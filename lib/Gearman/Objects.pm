use strict;

package Gearman::Client;
use fields (
            'job_servers',
            'js_count',
            'sock_cache',  # hostport -> socket
            );

package Gearman::Taskset;

use fields (
            'waiting',  # { handle => [Task, ...] }
            'client',   # Gearman::Client
            'need_handle',  # arrayref

            'default_sock',     # default socket (non-merged requests)
            'default_sockaddr', # default socket's ip/port

            'loaned_sock',      # { hostport => socket }

            );


package Gearman::Task;

use fields (
            # from client:
            'func',
            'argref',
            # opts from client:
            'uniq',
            'on_complete',
            'on_fail',
            'on_retry',
            'on_status',
            'on_post_hooks',   # used internally, when other hooks are done running, prior to cleanup
            'retry_count',
            'timeout',
            'high_priority',

            # from server:
            'handle',

            # maintained by this module:
            'retries_done',
            'is_finished',
            'taskset',
            'jssock',  # jobserver socket.  shared by other tasks in the same taskset,
                       # but not w/ tasks in other tasksets using the same Gearman::Client
            );


1;
