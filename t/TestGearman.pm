package TestGearman;
use base qw(Exporter);
@EXPORT = qw(start_server wait_for_port start_worker respawn_children pid_is_dead PORT %Children $NUM_SERVERS);
use strict;
use List::Util qw(first);;
use IO::Socket::INET;
use POSIX qw( :sys_wait_h );

our $Bin;
use FindBin qw( $Bin );

# TODO: use a variation of t/lib/GearTestLib::free_port to find 3 free ports
use constant PORT => 9050;

our $NUM_SERVERS = 1;

our %Children;

END { kill_children() }

sub start_server {
    my($port) = @_;
    my @loc = ("$Bin/../../../../server/gearmand",     # using svn
               "$Bin/../../../../../server/gearmand",  # using svn and 'disttest'
               '/usr/bin/gearmand',            # where some distros might put it
               '/usr/sbin/gearmand',           # where other distros might put it
               );
    my $server = first { -e $_ } @loc
        or return 0;

    my $ready = 0;
    local $SIG{USR1} = sub {
        $ready = 1;
    };

    my $pid = start_child([ $server, '-p' => $port, '-n' => $$ ]);
    $Children{$pid} = 'S';
    while (!$ready) {
        select undef, undef, undef, 0.10;
    }
    return $pid;
}

sub start_worker {
    my($port, $args) = @_;
    my $num_servers;
    unless (ref $args) {
        $num_servers = $args;
        $args        = {};
    }
    $num_servers ||= $args->{num_servers} || 1;
    my $worker = "$Bin/worker.pl";
    my $servers = join ',',
                  map '127.0.0.1:' . (PORT + $_),
                  0..$num_servers-1;
    my $ready = 0;
    my $pid;
    local $SIG{USR1} = sub {
        $ready = 1;
    };
    $pid = start_child([ $worker, '-s' => $servers, '-n' => $$, ($args->{prefix} ? ('-p' => $args->{prefix}) : ()) ]);
    $Children{$pid} = 'W';
    while (!$ready) {
        select undef, undef, undef, 0.10;
    }
    return $pid;
}

sub start_child {
    my($cmd) = @_;
    my $pid = fork();
    die $! unless defined $pid;
    unless ($pid) {
        exec $^X, '-Iblib/lib', '-Ilib', @$cmd or die $!;
    }
    $pid;
}


sub kill_children {
    kill INT => keys %Children;
}

sub wait_for_port {
    my($port) = @_;
    my $start = time;
    while (1) {
        my $sock = IO::Socket::INET->new(PeerAddr => "127.0.0.1:$port");
        return 1 if $sock;
        select undef, undef, undef, 0.25;
        die "Timeout waiting for port $port to startup" if time > $start + 5;
    }
}

sub pid_is_dead {
    my($pid) = @_;
    return if $pid == -1;
    my $type = delete $Children{$pid};
    if ($type eq 'W') {
        ## Right now we can only restart workers.
        start_worker(PORT, $NUM_SERVERS);
    }
}

sub respawn_children {
    for my $pid (keys %Children) {
        if (waitpid($pid, WNOHANG) > 0) {
            pid_is_dead($pid);
        }
    }
}

1;
