# Overview

The purpose of this plugin is to make it possible to make arbitrary changes to
the open-source bucardo files using sed. This came about because the developers
took away a behavior I was relying on to avoid data loss, and it was only one
line of sed to put it back.

Rather than hard-code this specific change, the `fork_bucardo` plugin was
developed to allow the user to run simple sed substitutions as needed.

# Usage

Update the config and run `fork_bucardo.apply_changes`. It will apply all uncommented-out changes.

# Sample Tweaks
    fork_bucardo:
        # Log deltas, i.e. changes awaiting replication, in normal mode. This used to be the default behavior for Bucardo.
        log_deltacounts:
            file: /usr/share/perl5/Bucardo.pm
            pattern: '.*Total delta count.*'
            replacement: '            $self->glog("Total delta count: $deltacount{all}", $deltacount{all} ? LOG_NORMAL : LOG_VERBOSE);'
        # Stop logging deltas, i.e. changes awaiting replication, in normal mode. This is the current default behavior for Bucardo.
        log_deltacounts_revert:
            file: /usr/share/perl5/Bucardo.pm
            pattern: '.*Total delta count.*'
            replacement: '            $self->glog("Total delta count: $deltacount{all}", LOG_VERBOSE);'
