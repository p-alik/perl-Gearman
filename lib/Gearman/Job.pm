# this is the object that's handed to the worker subrefs
package Gearman::Job;
$Gearman::Job::VERSION = '1.3.001';

use strict;
use warnings;

#TODO: retries?
#
use Gearman::Util;
use Carp             ();
use IO::Socket::INET ();


use fields (
    'func',
    'argref',
    'handle',
    'jss',    # job server's socket
);

sub new {
    my ($class, $func, $argref, $handle, $jss) = @_;
    my $self = $class;
    $self = fields::new($class) unless ref $self;

    $self->{func}   = $func;
    $self->{handle} = $handle;
    $self->{argref} = $argref;
    $self->{jss}    = $jss;
    return $self;
} ## end sub new

# ->set_status($numerator, $denominator) : $bool_sent_to_jobserver
sub set_status {
    my Gearman::Job $self = shift;
    my ($nu, $de) = @_;

    my $req = Gearman::Util::pack_req_command("work_status",
        join("\0", $self->{handle}, $nu, $de));
    die "work_status write failed"
        unless Gearman::Util::send_req($self->{jss}, \$req);
    return 1;
} ## end sub set_status

sub argref {
    my Gearman::Job $self = shift;
    return $self->{argref};
}

sub arg {
    my Gearman::Job $self = shift;
    return ${ $self->{argref} };
}

sub handle {
    my Gearman::Job $self = shift;
    return $self->{handle};
}


