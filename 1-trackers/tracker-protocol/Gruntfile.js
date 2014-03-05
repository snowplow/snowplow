module.exports = function(grunt) {

  grunt.initConfig({

    intern: {
      tests: {
        options: {
          runType: 'client',
          config: 'tests/intern.js'
        }
      }
    }
  });

  grunt.loadNpmTasks('intern');

  grunt.registerTask('default', ['intern'])

}
