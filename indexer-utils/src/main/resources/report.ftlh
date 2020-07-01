<html>
<head lang="en">
    <title>Apache Flink CI Dashboard</title>
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css" integrity="sha384-9aIt2nRpC12Uk9gS9baDl411NQApFmC26EwAOH8WgZl5MYYxFfc+NcPb1dKGj7Sk" crossorigin="anonymous">
    <link href="https://fonts.googleapis.com/icon?family=Material+Icons"
          rel="stylesheet">
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <style>
    </style>
</head>
<body>
<div class="px-3 py-3 pt-md-5 pb-md-4 mx-auto text-center" style="max-width: 700px;">
    <h3 class="display-4">Apache Flink CI Dashboard</h3>
</div>

<div class="container">
    <div class="card-deck mb-3 text-center">
        <div class="card mb-4 shadow-sm">
            <div class="card-header">
                <h4 class="my-0 font-weight-normal">E2E Test durations</h4>
            </div>
            <div class="card-body">
                <div class="container">
                    <table class="table">
                        <thead>
                        <tr>
                            <th scope="col">Test Name</th>
                            <th scope="col">Reported runs</th>
                            <th scope="col">Average execution time</th>
                            <th scope="col">Max execution time</th>
                            <th scope="col">Min execution time</th>
                        </tr>
                        </thead>
                        <tbody>
                        <#list aggTable as test>
                        <tr>
                            <td>${test.name}</td>
                            <td>${test.runs}</td>
                            <td>${test.avg}</td>
                            <td>${test.max}</td>
                            <td>${test.min}</td>
                        </tr>
                        <tr>
                            <td colspan="5">
                                TODO: Hide me
                                <div class="row text-left">
                                    <div class="col-sm-6">
                                        <b>Slow Tests</b>
                                        <ul class="list-group">
                                            <#list perTestStats[test.name].slowTests as stat>
                                                <li class="list-group-item"><a href="${stat.url}">${stat.name}</a></li>
                                            </#list>
                                        </ul>
                                    </div>
                                    <div class="col-sm-6">
                                        <b>Last Executions</b>
                                        <ul class="list-group">
                                            <#list perTestStats[test.name].lastExecutions as stat>
                                                <li class="list-group-item"><a href="${stat.url}">${stat.name}</a></li>
                                            </#list>
                                        </ul>
                                    </div>
                                </div>
                            </td>
                        </tr>
                        </#list>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>
</div>

    <small>Last updated ${generatedTime}</small>
</div>





<script src="https://code.jquery.com/jquery-3.3.1.slim.min.js" integrity="sha384-q8i/X+965DzO0rT7abK41JStQIAqVgRVzpbzo5smXKp4YfRvH+8abtTE1Pi6jizo" crossorigin="anonymous"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.14.7/umd/popper.min.js" integrity="sha384-UO2eT0CpHqdSJQ6hJty5KVphtPhzWj9WO1clHTMGa3JDZwrnQq4sF86dIHNDz0W1" crossorigin="anonymous"></script>
<script src="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/js/bootstrap.min.js" integrity="sha384-JjSmVgyd0p3pXB1rRibZUAYoIIy6OrQ6VrjIEaFf/nJGzIxFDsf4x0xIM+B07jRM" crossorigin="anonymous"></script>
<script lang="text/javascript">
    $(function () {
        $('[data-toggle="tooltip"]').tooltip()
    })

</script>
</body>
</html>