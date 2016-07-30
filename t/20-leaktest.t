use strict;
use warnings;

our $Bin;
use FindBin qw( $Bin );
use Gearman::Client;
use Storable qw( freeze );
use Test::More;
use IO::Socket::INET;
use POSIX qw( :sys_wait_h );
use List::Util qw(first);

use lib "$Bin/lib";
use Test::Gearman;

plan skip_all => "$0 in TODO";

if (!eval "use Devel::Gladiator; 1;") {
    plan skip_all => "This test requires Devel::Gladiator";
    exit 0;
}

my $tg = Test::Gearman->new(
    ip     => "127.0.0.1",
    daemon => $ENV{GEARMAND_PATH} || undef
);

$tg->is_perl_daemon()
    || plan skip_all => "test cases supported only by Gearman::Server";

$tg->start_servers() || plan skip_all => "Can't find server to test with";

($tg->check_server_connection(@{ $tg->job_servers }[0])) || plan skip_all => "connection check $_ failed";

plan tests => 7;

my $client = new_ok("Gearman::Client", [job_servers => $tg->job_servers()]);

my $tasks = $client->new_task_set;
ok(
    my $handle = $tasks->add_task(
        dummy => 'xxxx',
        {
            on_complete => sub { die "shouldn't complete"; },
            on_fail     => sub { warn "Failed...\n"; }
        }
    ),
    "got handle"
);

ok(my $sock = IO::Socket::INET->new(PeerAddr => @{ $tg->job_servers }[0]),
    "got raw connection");

my $num = sub {
    my $what = shift;
    my $n    = 0;
    print $sock "gladiator all\r\n";
    while (<$sock>) {
      print $_;
        last if /^\./;
        /(\d+)\s$what/ or next;
        $n = $1;
    }
    return $n;
};
is($num->("Gearman::Server::Client"),
    2, "2 clients connected (debug and caller)");

my $num_inets = $num->("IO::Socket::INET");

# a server change made this change from 3 to 4... so accept either.  just make
# sure it decreases by one later...
ok($num_inets == 3 || $num_inets == 4,
    "3 or 4 sockets (clients + listen) (got $num_inets)");
$tasks->cancel;

sleep(0.10);

my $num_inets2 = $num->("IO::Socket::INET");
is($num_inets2, $num_inets - 1, "2 sockets (client + listen)");
is($num->("Gearman::Server::Client"), 1, "1 client connected (debug)");

