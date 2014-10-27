require 'bundler/capistrano'
require 'capistrano/ext/multistage'

set :rbenv_ruby_version, '1.9.3-p547'

set :default_stage, "staging"
set :stages, %w( staging production )

# SSH Config
set :user, "ec2-user"
set :use_sudo, false

# Repo Config
set :git_enable_submodules, 1
set :repository,  "git@github.com:simplybusiness/snowplow.git"
set :application, "snowplow"
set :deploy_via, :remote_cache
set :deploy_to, "/home/#{fetch(:user, 'ec2-user')}/snowplow"
set :scm, :git
set :keep_releases, 10
set :branch, `git branch`.match(/\* (\S+)\s/m)[1]

namespace :deploy do
  task :install_bundler do
    puts "Installing bundler"
    run "gem install bundler"
  end
end
