server "10.10.1.154", :app, :web
role :db, "10.10.1.154", :primary => true

# FIXME remove the above hardcoded host