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

use lib "$Bin/lib";
use GearTestLib;
use constant NUM_SERVERS => 3;

if (! eval "use Devel::Gladiator; 1;") {
    plan skip_all => "This test requires Devel::Gladiator";
    exit 0;
}

my $s1 = Test::GearServer->new;
if (! $s1) {
    plan skip_all => "Can't find server to test with";
    exit 0;
}

plan tests => 6;

my $client = Gearman::Client->new;
$client->job_servers($s1->ipport);

my $tasks = $client->new_task_set;
my $handle = $tasks->add_task(dummy => 'xxxx',
                              on_complete => sub { die "shouldn't complete"; },
                              on_fail => sub { warn "Failed...\n"; });


ok($handle, "got handle");
my $sock = IO::Socket::INET->new(PeerAddr => $s1->ipport);
ok($sock, "got raw connection");

my $num = sub {
    my $what = shift;
    my $n = 0;
    print $sock "gladiator all\r\n";
    while (<$sock>) {
        last if /^\./;
        /(\d+)\s$what/ or next;
        $n = $1;
    }
    return $n;
};

is($num->("Gearman::Server::Client"), 2, "2 clients connected (debug and caller)");

my $num_inets = $num->("IO::Socket::INET");
# a server change made this change from 3 to 4... so accept either.  just make
# sure it decreases by one later...
ok($num_inets == 3 || $num_inets == 4, "3 or 4 sockets (clients + listen) (got $num_inets)");
$tasks->cancel;

sleep(0.10);

my $num_inets2 = $num->("IO::Socket::INET");
is($num_inets2, $num_inets-1, "2 sockets (client + listen)");
is($num->("Gearman::Server::Client"), 1, "1 client connected (debug)");


__END__



eval { $client->do_task(sum => []) };
