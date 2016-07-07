package TestGearman;
use base qw(Exporter);
@EXPORT = qw(
    free_ports
    start_server
    check_server_connection
    start_worker
    respawn_children
    pid_is_dead
    %Children
);

use strict;
use warnings;

use IO::Socket::INET;
use POSIX qw( :sys_wait_h );

use FindBin qw( $Bin );

our %Children;

END { kill_children() }

sub free_ports {
    my ($la, $count) = @_;
    $count||=1;
    my @p;
    for (1 .. $count) {
        my $fp = _free_port($la);
        $fp && push @p, $fp;
    }
    return @p;
} ## end sub free_ports

sub _free_port {
    my ($la, $port) = shift;
    my ($type, $retry, $sock) = ("tcp", 5);
    do {
        unless ($port) {
            $port = int(rand(20000)) + 30000;
        }

        IO::Socket::INET->new(
            LocalAddr => $la,
            LocalPort => $port,
            Proto     => $type,
            ReuseAddr => 1
        ) or undef($port);

    } until ($port || --$retry == 0);

    return $port;
} ## end sub _free_port

sub start_server {
    my ($server, $port) = @_;
    $server ||= qx/which gearmand/;
    ($server && $port) || return;

    chomp $server;

    (-e $server) || return;

    my $ready = 0;
    local $SIG{USR1} = sub {
        $ready = 1;
    };

    my $pid = start_child([$server, '-p' => $port, '-n' => $$]);
    $Children{$pid} = 'S';
    while (!$ready) {
        select undef, undef, undef, 0.10;
    }
    return $pid;
} ## end sub start_server

sub start_worker {
    my ($job_servers, $args) = @_;
    unless (ref $args) {
        $args = {};
    }

    my $worker = "$Bin/worker.pl";
    my $servers = join ',', @{$job_servers};
    my $ready = 0;
    my $pid;
    local $SIG{USR1} = sub {
        $ready = 1;
    };
    $pid = start_child(
        [
            $worker,
            '-s' => $servers,
            '-n' => $$,
            ($args->{prefix} ? ('-p' => $args->{prefix}) : ())
        ]
    );
    $Children{$pid} = 'W';
    while (!$ready) {
        select undef, undef, undef, 0.10;
    }
    return $pid;
} ## end sub start_worker

sub start_child {
    my ($cmd) = @_;
    my $pid = fork();
    die $! unless defined $pid;
    unless ($pid) {
        exec $^X, '-Iblib/lib', '-Ilib', @$cmd or die $!;
    }
    $pid;
} ## end sub start_child

sub kill_children {
    kill INT => keys %Children;
}

sub check_server_connection {
    my ($pa) = @_;
    my $start = time;
    my $sock;
    do {
        $sock = IO::Socket::INET->new(PeerAddr => $pa);
        select undef, undef, undef, 0.25;
        die "Timeout waiting for peer address $pa" if time > $start + 5;
    } until ($sock);

    return defined($sock);
} ## end sub check_server_connection

sub pid_is_dead {
    my ($pid) = shift;
    warn "pid $pid";
    return if $pid == -1;
    my $type = delete $Children{$pid};
    if ($type eq 'W') {
        ## Right now we can only restart workers.
        start_worker(@_);
    }
} ## end sub pid_is_dead

sub respawn_children {
    for my $pid (keys %Children) {
        if (waitpid($pid, WNOHANG) > 0) {
            pid_is_dead($pid, @_);
        }
    }
} ## end sub respawn_children

1;
