use strict;
use warnings;
use Test::More tests => 2;
use Gearman::Client;

our $last_packet = undef;

my $parser = Gearman::ResponseParser::Test->new();

test_packet("\0RES\0\0\0\x0a\0\0\0\x01!", {
    len => 1,
    blobref => \"!", #"
    type => 'no_job',
});

test_packet("\0RES\0\0\0\x0a\0\0\0\0", {
    len => 0,
    blobref => \"", #"
    type => 'no_job',
});

sub test_packet {
    my ($data, $expected) = @_;

    my $test_name = "Parsing ".enc($data);

    $last_packet = undef;
    $parser->parse_data(\$data);
    is_deeply($last_packet, $expected, $test_name);
}

sub enc {
    my $data = $_[0];
    $data =~ s/([\W])/"%" . uc(sprintf("%2.2x",ord($1)))/eg;
    return $data;
}

package Gearman::ResponseParser::Test;

use Gearman::ResponseParser;
use base qw(Gearman::ResponseParser);

sub on_packet {
    $main::last_packet = $_[1];
}
