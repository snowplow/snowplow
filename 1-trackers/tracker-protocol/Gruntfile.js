module.exports = function(grunt) {

  grunt.initConfig({

    intern: {
      tests: {
        options: {
          runType: 'client',
          config: 'tests/intern.js'
        }
      }
    },

    json_schema: {
      test: {
        files: {
          'schemas/ad_impression.json': ['inputs/ad_impression.json']
        }
      }
    }
  })

  grunt.loadNpmTasks('intern');

  grunt.registerTask('default', ['intern']);

}
