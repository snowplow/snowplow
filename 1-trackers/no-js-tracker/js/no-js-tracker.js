$(function() {
	$(".button").click(function() {
		// First clear the output div of any contents (e.g. from tags that were inserted last time the form was submitted)
		$('#output').html("");
		alert('starting function!');
		var applicationId = $("input#applicationId").val();
		var cloudfrontSubdomain = $("input#cloudfrontSubdomain").val();
		var selfHostedCollectorUrl = $("input#selfHostedCollectorUrl").val();
		var pageTitle = $("input#pageTitle").val();
		var pageUrl = $("input#pageUrl").val();

		var embedCode = applicationId + " " + cloudfrontSubdomain + " " + selfHostedCollectorUrl + " " + pageUrl + " " + pageTitle + " oooh wee...";

		$('#output').append($('<h2>The embed code is:' + embedCode + '</h2>'));

		return false;
	});
});
