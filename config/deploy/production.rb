server "10.10.1.209", :app, :web
role :db, "10.10.1.209", :primary => true

# FIXME remove the above hardcoded host