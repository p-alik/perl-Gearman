#!/usr/bin/perl

use strict;
use Gearman::Client;
use Test::More;
use lib 't';
use Time::HiRes;
use IO::Socket::INET;

{
    my $start_time = [Time::HiRes::gettimeofday];
    my $sock = IO::Socket::INET->new(PeerAddr => "192.0.2.1:1", Timeout => 2);
    my $delta = Time::HiRes::tv_interval($start_time);

    if ($sock) {
        plan skip_all => "Somehow we connected to the TEST-NET block. This should be impossible.";
        exit 0;
    } elsif ($delta < 1 || $delta > 3) {
        plan skip_all => "Socket timeouts aren't behaving, we can't trust this test in that scenario.";
        exit 0;
    }
    plan tests => 10;
}

# Testing exponential backoff
{
    my $client = Gearman::Client->new(exceptions => 1);
    $client->job_servers('192.0.2.1:1'); # doesn't connect

    # 1 second backoff (1 ** 2)
    time_between(.9, 1.1, sub { $client->do_task(anything => '') }, "Fresh server list, slow failure");
    time_between(undef, .1, sub { $client->do_task(anything => '') }, "Backoff for 1s, fast failure");
    sleep 2;

    # 4 second backoff (2 ** 2)
    time_between(.9, 1.1, sub { $client->do_task(anything => '') }, "Backoff cleared, slow failure");
    time_between(undef, .1, sub { $client->do_task(anything => '') }, "Backoff for 4s, fast failure (1/2)");
    sleep 2;
    time_between(undef, .1, sub { $client->do_task(anything => '') }, "Backoff for 4s, fast failure (2/2)");
    sleep 2;
    time_between(.9, 1.1, sub { $client->do_task(anything => '') }, "Backoff cleared, slow failure");

    # Now we reset the server list again and see if we have a slow backoff again.
    $client->job_servers('192.0.2.2:1'); # doesn't connect

    # Fresh server list, backoff will be 1 second (1 ** 2) after the first failure.
    time_between(.9, 1.1, sub { $client->do_task(anything => '') }, "Changed server list, slow failure");
    time_between(undef, .1, sub { $client->do_task(anything => '') }, "Backoff for 1s, fast failure");
    sleep 2;

    # Now we've cleared the timeout (1 second), mis-connect again, and test to see if we back off for 4 seconds (2 ** 2).
    time_between(.9, 1.1, sub { $client->do_task(anything => '') }, "Backoff cleared, slow failure");
    time_between(undef, .1, sub { $client->do_task(anything => '') }, "Backoff again, fast failure");
}

sub time_between {
    my $low = shift;
    my $high = shift;
    my $cv = shift;
    my $message = shift;

    my $starttime = [Time::HiRes::gettimeofday];
    $cv->();
    my $delta = Time::HiRes::tv_interval($starttime);

    my $fullmessage;
    if (defined $low) {
        if (defined $high) {
            $fullmessage = "Timed between $low and $high: $message";
        } else {
            $fullmessage = "Timed longer than $low: $message";
        }
    } else {
        $fullmessage = "Timed shorter than $high: $message";
    }

    if (defined $low && $low > $delta) {
        fail($fullmessage);
        return;
    }
    if (defined $high && $high < $delta) {
        fail($fullmessage);
        return;
    }
    pass($fullmessage);
}
