#!/usr/bin/perl

use strict;
our $Bin;
use FindBin qw( $Bin );
use Gearman::Client;
use Storable qw( freeze );
use Test::More;
use IO::Socket::INET;
use POSIX qw( :sys_wait_h );
use List::Util qw(first);;

use constant PORT => 9000;
our %Children;

END { kill_children() }

if (start_server(PORT)) {
    plan tests => 6;
} else {
    plan skip_all => "Can't find server to test with";
    exit 0;
}

wait_for_port(PORT);

{
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => PORT,
    );
    ok($sock, "connect to jobserver");

    $sock->write( "MAXQUEUE long 1\n" );
    my $input = $sock->getline();
    ok($input =~ m/^OK\b/i);
}

start_worker(PORT);

my $client = Gearman::Client->new;
isa_ok($client, 'Gearman::Client');

$client->job_servers('127.0.0.1:' . PORT);

my $tasks = $client->new_task_set;
isa_ok($tasks, 'Gearman::Taskset');

my $failed = 0;
my $completed = 0;

foreach my $iter (1..5) {
    my $handle = $tasks->add_task('long', $iter, {
        on_complete => sub { $completed++ },
        on_fail => sub { $failed++ }
    });
}
$tasks->wait;

is($completed, 2, 'number of success'); # One starts immediately and on the queue
is($failed, 3, 'number of failure'); # All the rest

sub start_server {
    my($port) = @_;
    my @loc = ("$Bin/../../../../server/gearmand",  # using svn
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
    my($port) = @_;
    my $worker = "$Bin/worker.pl";
    my $servers = '127.0.0.1:' . PORT;
    my $ready = 0;
    my $pid;
    local $SIG{USR1} = sub {
        $ready = 1;
    };
    $pid = start_child([ $worker, '-s' => $servers, '-n' => $$ ]);
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
        exec 'perl', '-Iblib/lib', '-Ilib', @$cmd or die $!;
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

